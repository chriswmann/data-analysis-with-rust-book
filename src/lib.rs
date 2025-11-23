//! Main entrypoint orchestrating the book's full data ingestion pipeline.
//!
//! This binary downloads the ONS census teaching dataset, expands it to ~60 million rows,
//! streams it into Postgres and MinIO, then verifies the objects are reachable. Each stage
//! guards itself with filesystem or database existence checks so repeated runs remain fast
//! and idempotent.

use tracing::{debug, info};
use tracing_subscriber::prelude::*;

mod cli;
mod data;
mod pipeline;

pub use crate::cli::Args;
pub use crate::data::blob::{S3Client, S3Config};
pub use crate::pipeline::Pipeline; // Re-export Pipeline since we need these for the examples
pub use crate::pipeline::context::Context; // Re-export Context since we need this for the examples
pub use crate::pipeline::stages::DatasetConfig; // Re-export DatasetConfig since we need this for the examples
pub use data::rdbms::PostgresConn;
use data::rdbms::drop_table_if_exists;

/// Main ETL flow, orchestrating census data acquisition, expansion, and persistence
/// into both Postgres (via COPY protocol) and MinIO (via S3 multipart upload).
///
/// The flow is split into five stages:
/// 1. Parse CLI arguments and prepare local paths.
/// 2. Either load a cached parquet file or download the raw CSV, expand it, and persist.
/// 3. Connect to Postgres and populate the census table if row counts are incomplete.
/// 4. Configure S3 and optionally recreate the bucket.
/// 5. Upload the large parquet artefact if it's missing from object storage.
pub async fn run(args: Args) -> anyhow::Result<()> {
    // Initialise structured logging with tracing, letting RUST_LOG control verbosity
    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Define all of the local file paths, so we can check if they exist
    // to avoid unneeded processing

    // Stage 2: Configure and connect to Postgres (expected to be listening on port 6543)
    let postgres_conn = PostgresConn {
        user: "postgres".into(),
        password: "postgres".into(),
        host: "localhost".into(),
        port: 6543,
        database: "dair".into(),
    };

    let db_uri = postgres_conn.get_db_connection_string();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(20)
        .connect(&db_uri)
        .await?;

    if args.delete_table {
        debug!("Dropping census table.");
        drop_table_if_exists(&pool, "census").await?;
    }
    let s3_config = S3Config {
        region: "eu-west-1".into(),
        url: "http://127.0.0.1:9000".into(),
        username: "minioadmin".into(),
        password: "minioadmin".into(),
    };

    let s3_client = S3Client::new(&s3_config);

    let bucket_name = "census";
    let dataset_config = DatasetConfig::default();
    let ctx = Context::builder(&args, dataset_config)
        .with_cache_dir(args.get_data_path())
        .with_db_pool(pool)
        .with_s3_config(s3_config)
        .with_s3_client(s3_client.clone())
        .with_pg_conn(postgres_conn)
        .with_table_name(bucket_name.into())
        .with_bucket_name(bucket_name.into())
        .build()?;
    if args.delete_bucket {
        s3_client.delete_bucket(bucket_name).await?;
    };

    let pipeline = Pipeline::builder()
        .with_postgres_persistence()
        .with_s3_persistence()
        .with_postgres_retrieval()
        .with_s3_retrieval()
        .with_simple_filter_data()
        .with_complex_filter_data()
        .finish();

    let ctx = pipeline.run(ctx).await?;

    debug!("Context:\n{}", ctx);
    // List all objects in the bucket to confirm the upload succeeded
    let bucket_objects = s3_client.list_objects(bucket_name).await?;
    info!(
        "Found these objects in the {} bucket:\n{:#?}",
        bucket_name, &bucket_objects
    );

    Ok(())
}
