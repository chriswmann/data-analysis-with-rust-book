use anyhow::Result;
use polars::prelude::{CsvWriter, DataType, LazyFrame, SchemaExt, SerWriter};
use sqlx::{Pool, Postgres};
use tracing::debug;

pub(crate) struct PostgresConn {
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) host: String,
    pub(crate) port: i32,
    pub(crate) database: String,
}

impl PostgresConn {
    pub(crate) fn get_connection_string(&self) -> String {
        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}

#[tracing::instrument(skip(lf))]
pub(crate) async fn load_lf_dynamic(
    pool: &Pool<Postgres>,
    lf: LazyFrame,
    table_name: &str,
) -> Result<()> {
    let mut lf = lf;
    let schema = lf.collect_schema()?;

    // Extract column names and types
    let column_names: Vec<String> = schema.iter_fields().map(|f| f.name().to_string()).collect();
    let quoted_column_names: Vec<String> =
        column_names.iter().map(|n| format!("\"{}\"", n)).collect();
    let column_types: Vec<String> = schema
        .iter_fields()
        .map(|f| map_polars_type_to_pg(f.dtype()))
        .collect();

    create_dynamic_table(pool, table_name, &quoted_column_names, &column_types).await?;

    // Get a raw connection from the pool for the COPY protocol
    let mut conn = pool.acquire().await?;

    // Start COPY FROM STDIN
    let copy_query = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT CSV)",
        table_name,
        column_names.join(", "),
    );
    debug!("Copy query: {}", &copy_query);

    // Get the copy in stream from the connection
    let mut copy_in = conn.copy_in_raw(&copy_query).await?;

    const ROWS_PER_BATCH: u32 = 100_000;
    let mut offset = 0;

    loop {
        debug!("SQL COPY offset: {}", offset);
        // Evaluate one window of the lazy plan
        let chunk_lf = lf.clone().slice(offset, ROWS_PER_BATCH);
        let chunk_df = tokio::task::spawn_blocking(move || chunk_lf.collect()).await??;

        let chunk_df_height = chunk_df.height();
        if chunk_df_height == 0 {
            break;
        }
        // Render the chunk to CSV bytes in a blocking task
        let csv_bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let mut df = chunk_df.clone();
            let mut buffer = Vec::new();
            CsvWriter::new(&mut buffer)
                .include_header(false)
                .finish(&mut df)?;
            Ok(buffer)
        })
        .await??;

        copy_in.send(csv_bytes).await?;
        offset += chunk_df_height as i64;
    }

    // Finish the COPY FROM STDIN
    copy_in.finish().await?;

    Ok(())
}

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

fn map_polars_type_to_pg(dtype: &DataType) -> String {
    match dtype {
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float64 => "DOUBLE PRECISION".into(),
        DataType::String => "TEXT".into(),
        _ => "TEXT".into(),
    }
}

#[tracing::instrument]
pub(crate) async fn drop_table_if_exists(
    pool: &Pool<Postgres>,
    table_name: &str,
) -> Result<(), sqlx::Error> {
    // Validate the table name first
    if !table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(sqlx::Error::Protocol(format!(
            "Invalid table name ({table_name}): only alphanumeric characters and underscores allowed."
        )));
    }

    // Use format! to build the query with validated identifier
    // Note: SQL parameters ($1, $2) don't work for table names - they only work for values
    let query = format!("drop table if exists {}", table_name);
    debug!("Drop table query: {}", &query);

    sqlx::query(&query).execute(pool).await?;

    Ok(())
}

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
    "#,
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
