//! Main entrypoint orchestrating the book's full data ingestion pipeline.
//!
//! This binary downloads the ONS census teaching dataset, expands it to ~60 million rows,
//! streams it into Postgres and MinIO, then verifies the objects are reachable. Each stage
//! guards itself with filesystem or database existence checks so repeated runs remain fast
//! and idempotent.

use tokio::fs;
use tracing::debug;

mod cli;
mod data;
mod pipeline;

pub use crate::cli::Args;
use data::RAW_URL;
use data::etl::{
    get_data, shorten_census_column_names, try_read_parquet_to_lf, write_lf_to_csv,
    write_lf_to_parquet,
};
use data::rdbms::{PostgresConn, drop_table_if_exists, load_lf_dynamic};
use data::synthesise::expand_census_data;

use crate::data::blob::{S3Client, S3Config};
use crate::data::rdbms::get_table_count_if_exists;

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
    let raw_data_path = args.get_raw_data_path();
    let large_data_path = args.get_data_path().join("large");
    let raw_census_csv_path = raw_data_path.join("census.csv");
    let large_census_parquet_path = large_data_path.join("census.parquet");
    // Stage 1: Either load the pre-expanded dataset or build it from scratch by fetching
    // the ONS source, persisting raw copies, then expanding and re-writing
    let lf = if !large_census_parquet_path.exists() {
        println!("Large census parquet file not found - downloading and processing...");
        fs::create_dir_all(&raw_data_path).await?;

        // Download the ONS micro census teaching sample from the public endpoint
        let micro_census_data_url = RAW_URL;
        let lf = get_data(raw_census_csv_path.as_path(), micro_census_data_url).await?;

        // Preview the raw data before any transformations
        let raw_lf = lf.clone();
        let raw_data_head =
            tokio::task::spawn_blocking(move || raw_lf.limit(5).collect()).await??;
        println!("Head: {:?}", raw_data_head);

        // Persist the original dataset in both CSV and Parquet formats for archival purposes
        write_lf_to_csv(lf.clone(), &raw_data_path.join("census.csv")).await?;
        write_lf_to_parquet(lf.clone(), &raw_data_path.join("census.parquet")).await?;
        fs::create_dir_all(&large_data_path).await?;

        // Abbreviate column names and synthetically expand to ~60 million rows
        let lf = shorten_census_column_names(lf);
        let lf = expand_census_data(lf, 100).await?;

        // Write the expanded dataset to disk for future runs
        write_lf_to_csv(lf.clone(), &large_data_path.join("census.csv")).await?;
        write_lf_to_parquet(lf.clone(), &large_data_path.join("census.parquet")).await?;
        lf
    } else {
        // Fast path: rehydrate the expanded frame from disk
        println!("Large census parquet file found - loading...");
        try_read_parquet_to_lf(&large_census_parquet_path)?
    };

    // Preview the expanded data to confirm the transformation succeeded
    let large_lf = lf.clone();
    let large_data_head =
        tokio::task::spawn_blocking(move || large_lf.limit(5).collect()).await??;
    println!("Head after expansion: {:?}", large_data_head);

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

    // There should be 60_435_100 rows in the census table after expansion. If the count
    // is lower or the rebuild flag was set, drop and reload the table via COPY protocol.
    let census_count = get_table_count_if_exists("census", &pool).await?;
    if args.rebuild_db || census_count < 60_435_100 {
        debug!("Dropping census table.");
        drop_table_if_exists(&pool, "census").await?;

        println!("Loading CSV file into postgres...");
        load_lf_dynamic(&pool, lf.clone(), "census").await?;
    } else {
        println!("{} rows in census table.", census_count);
    }

    // Stage 3: Configure S3 to point at a local MinIO instance (port 9000), using the
    // default admin credentials and the eu-west-1 region constraint
    let s3_config = S3Config {
        region: "eu-west-1".into(),
        url: "http://127.0.0.1:9000".into(),
        username: "minioadmin".into(),
        password: "minioadmin".into(),
    };

    let s3_client = S3Client::new(s3_config, "census".into());

    // If the user passed --delete-bucket, remove the existing bucket and all objects
    // before continuing
    if args.delete_bucket {
        s3_client.delete_bucket().await?;
    };

    // Ensure the census bucket exists, creating it if necessary
    if !s3_client.bucket_exists().await? {
        s3_client.create_bucket().await?;
    };

    // Upload the large parquet file to S3 using multipart streaming if not already present
    let object_exists = s3_client.object_exists("large/census.parquet").await?;
    if !object_exists {
        s3_client
            .stream_chunked_parquet_to_s3(
                large_census_parquet_path.to_str().unwrap(),
                "large/census.parquet",
            )
            .await?;
    }

    // List all objects in the bucket to confirm the upload succeeded
    let bucket_objects = s3_client.list_objects().await?;
    println!(
        "Found these objects in the {} bucket:\n{:#?}",
        s3_client.get_bucket_name(),
        &bucket_objects
    );

    Ok(())
}
