//! Helpers for loading Polars frames into PostgreSQL via the COPY protocol.
//!
//! The module targets high-throughput ingestion by streaming CSV chunks directly through
//! `COPY FROM STDIN`, bypassing row-by-row inserts and taking advantage of Postgres's
//! optimised bulk-load path.

use anyhow::Result;
use connectorx::prelude::*;
use polars::prelude::{CsvWriter, DataType, IntoLazy, LazyFrame, SchemaExt, SerWriter};
use sqlx::{Pool, Postgres};
use tracing::debug;

/// Encapsulates the connection parameters for a Postgres database.
pub(crate) struct PostgresConn {
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) host: String,
    pub(crate) port: i32,
    pub(crate) database: String,
}

impl PostgresConn {
    /// Formats the connection details into a standard PostgreSQL connection URI.
    pub(crate) fn get_db_connection_string(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}

/// Streams a `LazyFrame` into Postgres using the COPY protocol, automatically inferring
/// and creating the target table schema.
///
/// The frame is sliced into 100k-row chunks to keep memory usage bounded, each chunk
/// is rendered to headerless CSV on a blocking task, then fed into the `COPY FROM STDIN`
/// stream. This keeps the async runtime free while Polars performs CPU-bound work and
/// allows the database to ingest data faster than row-by-row inserts.
#[tracing::instrument(skip(lf))]
pub(crate) async fn load_lf_dynamic(
    pool: &Pool<Postgres>,
    lf: LazyFrame,
    table_name: &str,
) -> Result<()> {
    let mut lf_for_schmea = lf.clone();
    let schema = tokio::task::spawn_blocking(move || lf_for_schmea.collect_schema()).await??;

    // Extract column names and types for dynamic DDL
    let column_names: Vec<String> = schema.iter_fields().map(|f| f.name().to_string()).collect();
    let quoted_column_names: Vec<String> =
        column_names.iter().map(|n| format!("\"{}\"", n)).collect();
    let column_types: Vec<String> = schema
        .iter_fields()
        .map(|f| map_polars_type_to_pg(f.dtype()))
        .collect();

    create_dynamic_table(pool, table_name, &quoted_column_names, &column_types).await?;

    // Acquire a raw connection from the pool for the COPY protocol
    let mut conn = pool.acquire().await?;

    // Start COPY FROM STDIN
    let copy_query = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT CSV)",
        table_name,
        column_names.join(", "),
    );
    debug!("Copy query: {}", &copy_query);

    let mut copy_in = conn.copy_in_raw(&copy_query).await?;

    const ROWS_PER_BATCH: u32 = 100_000;
    let mut offset = 0;

    loop {
        debug!("SQL COPY offset: {}", offset);
        // Evaluate one window of the lazy plan
        let chunk_lf = lf.clone().slice(offset, ROWS_PER_BATCH);

        // Render the chunk to CSV bytes in a blocking task
        let (csv_bytes, chunk_df_height) =
            tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, usize)> {
                let mut chunk_df = chunk_lf.collect()?;
                let chunk_df_height = chunk_df.height();
                let mut buffer = Vec::new();
                CsvWriter::new(&mut buffer)
                    .include_header(false)
                    .finish(&mut chunk_df)?;
                Ok((buffer, chunk_df_height))
            })
            .await??;

        if chunk_df_height == 0 {
            break;
        }

        copy_in.send(csv_bytes).await?;
        offset += chunk_df_height as i64;
    }

    // Finish the COPY FROM STDIN
    copy_in.finish().await?;

    Ok(())
}

/// Creates a table with dynamically inferred column types, derived from the Polars schema.
///
/// Uses `IF NOT EXISTS` to avoid clobbering an existing table with the same name.
#[tracing::instrument]
async fn create_dynamic_table(
    pool: &Pool<Postgres>,
    table_name: &str,
    column_names: &[String],
    column_types: &[String],
) -> Result<()> {
    let columns: Vec<String> = column_names
        .iter()
        .zip(column_types)
        .map(|(name, dtype)| format!("{} {}", name, dtype))
        .collect();

    let create_query = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        table_name,
        columns.join(", ")
    );

    debug!("Create table query: {}", &create_query);
    sqlx::query(&create_query).execute(pool).await?;
    Ok(())
}

/// Maps a Polars `DataType` to its nearest Postgres equivalent, falling back to `TEXT`
/// for unsupported types to ensure the ingestion never fails.
fn map_polars_type_to_pg(dtype: &DataType) -> String {
    match dtype {
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float64 => "DOUBLE PRECISION".into(),
        DataType::String => "TEXT".into(),
        _ => "TEXT".into(),
    }
}

/// Drops the specified table after validating the name to prevent SQL injection.
///
/// Table names cannot be parameterised in SQL (unlike values), so the identifier is
/// injected into the query string after confirming it contains only alphanumerics and
/// underscores.
#[tracing::instrument]
pub(crate) async fn drop_table_if_exists(
    pool: &Pool<Postgres>,
    table_name: &str,
) -> Result<(), sqlx::Error> {
    // Validate the table name to avoid SQL injection and to ensure the column names are nicely styled
    if !table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(sqlx::Error::Protocol(format!(
            "Invalid table name ({table_name}): only alphanumeric characters and underscores allowed."
        )));
    }

    // Build the query with the validated identifier
    // Note: SQL parameters ($1, $2) don't work for table names - they only work for values
    let query = format!("drop table if exists {}", table_name);
    debug!("Drop table query: {}", &query);

    sqlx::query(&query).execute(pool).await?;

    Ok(())
}

/// Returns the row count for a table if it exists in the public schema, otherwise zero.
///
/// First checks `information_schema.tables` using a parameterised query, then issues
/// a `COUNT(1)` on the validated table name to fetch the cardinality.
pub(crate) async fn get_table_count_if_exists(
    table_name: &str,
    pool: &Pool<Postgres>,
) -> Result<i64> {
    let exists: bool = sqlx::query_scalar(
        r#"
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = $1
    )
    "#
        .trim(),
    )
    .bind(table_name)
    .fetch_one(pool)
    .await?;

    let count_query = format!(r#"SELECT COUNT(1) FROM {}"#, table_name);
    let row_count: i64 = if exists {
        sqlx::query_scalar(&count_query).fetch_one(pool).await?
    } else {
        0
    };

    Ok(row_count)
}

pub(crate) async fn load_lf_from_postgres(
    table_name: &str,
    pg_conn: &PostgresConn,
) -> Result<LazyFrame> {
    let uri = pg_conn.get_db_connection_string();
    let table_name = table_name.to_string();

    let lf = tokio::task::spawn_blocking(move || {
        let source_conn = SourceConn::try_from(uri.as_str())?;
        // Prepare query (london, aged 15 years and under)
        let query_str = format!(
            "SELECT * FROM {} WHERE region = 'E12000007' and age_group = 1",
            table_name
        );
        let query = &[CXQuery::from(query_str.as_str())];

        // ConnectorX query PostgreSQL and return Polars object
        let lf = get_arrow(&source_conn, None, query, None)?.polars()?.lazy();
        Ok::<_, anyhow::Error>(lf)
    })
    .await??;
    Ok(lf)
}
