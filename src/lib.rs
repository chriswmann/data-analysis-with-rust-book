//! Main entrypoint orchestrating the book's full data ingestion pipeline.
//!
//! This binary downloads the ONS census teaching dataset, expands it to ~60 million rows,
//! streams it into Postgres and MinIO, then verifies the objects are reachable. Each stage
//! guards itself with filesystem or database existence checks so repeated runs remain fast
//! and idempotent.

use tracing::debug;

mod cli;
mod data;
mod pipeline;

pub use crate::cli::Args;
use crate::data::blob::{S3Client, S3Config};
use crate::data::rdbms::get_table_count_if_exists;
use crate::pipeline::Pipeline;
use crate::pipeline::context::Context;
use data::rdbms::{PostgresConn, drop_table_if_exists, load_lf_dynamic};

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

    let db_uri = postgres_conn.get_connection_string();
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

    let s3_client = S3Client::new(s3_config, "census".into());

    if args.delete_bucket {
        s3_client.delete_bucket().await?;
    };

    let ctx = Context::builder(&args)
        .with_cache_dir(args.get_data_path())
        .with_db_pool(pool)
        .with_s3_client(s3_client)
        .build()?;

    let pipeline = Pipeline::new().await?;

    let ctx = pipeline.run(ctx).await?;

    debug!("Context:\n{}", ctx);
    // // There should be 60_435_100 rows in the census table after expansion. If the count
    // // is lower or the rebuild flag was set, drop and reload the table via COPY protocol.
    // let census_count = get_table_count_if_exists("census", &pool).await?;
    // if args.rebuild_db || census_count < 60_435_100 {
    //     debug!("Dropping census table.");
    //     drop_table_if_exists(&pool, "census").await?;
    //
    //     println!("Loading CSV file into postgres...");
    //     load_lf_dynamic(&pool, lf.clone(), "census").await?;
    // } else {
    //     println!("{} rows in census table.", census_count);
    // }
    //
    // // Stage 3: Configure S3 to point at a local MinIO instance (port 9000), using the
    // // default admin credentials and the eu-west-1 region constraint
    //
    //
    // // Ensure the census bucket exists, creating it if necessary
    // if !s3_client.bucket_exists().await? {
    //     s3_client.create_bucket().await?;
    // };
    //
    // // Upload the large parquet file to S3 using multipart streaming if not already present
    // let object_exists = s3_client.object_exists("large/census.parquet").await?;
    // if !object_exists {
    //     s3_client
    //         .stream_chunked_parquet_to_s3(
    //             large_census_parquet_path.to_str().unwrap(),
    //             "large/census.parquet",
    //         )
    //         .await?;
    // }
    //
    // // List all objects in the bucket to confirm the upload succeeded
    // let bucket_objects = s3_client.list_objects().await?;
    // println!(
    //     "Found these objects in the {} bucket:\n{:#?}",
    //     s3_client.get_bucket_name(),
    //     &bucket_objects
    // );

    Ok(())
}
