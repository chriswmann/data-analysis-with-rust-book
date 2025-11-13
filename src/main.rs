use clap::Parser;
use tokio::fs;
use tracing::debug;
use tracing_subscriber::prelude::*;

mod cli;
mod data;

use cli::Args;
use data::etl::{
    get_data, shorten_census_column_names, try_read_parquet_to_lf, write_lf_to_csv,
    write_lf_to_parquet,
};
use data::rdbms::{PostgresConn, drop_table_if_exists, load_lf_dynamic};
use data::synthesise::expand_census_data;

use crate::data::blob::{S3Client, S3Config};
use crate::data::rdbms::get_table_count_if_exists;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Define all of the local file paths, so we can check if they exist
    // to avoid unneeded processing
    let args = Args::parse();
    let raw_data_path = args.get_raw_data_path();
    let large_data_path = args.get_data_path().join("large");
    let raw_census_csv_path = raw_data_path.join("census.csv");
    let large_census_parquet_path = large_data_path.join("census.parquet");
    let lf = if !large_census_parquet_path.exists() {
        println!("Large census parquet file not found - downloading and processing...");
        fs::create_dir_all(&raw_data_path).await?;
        let micro_census_data_url = "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/publicmicrodatateachingsampleenglandandwalescensus2021/current/upload-publicmicrodatateachingsample.csv";
        let lf = get_data(raw_census_csv_path.as_path(), micro_census_data_url).await?;
        let raw_lf = lf.clone();
        let raw_data_head =
            tokio::task::spawn_blocking(move || raw_lf.limit(5).collect()).await??;
        println!("Head: {:?}", raw_data_head);
        write_lf_to_csv(lf.clone(), &raw_data_path.join("census.csv")).await?;
        write_lf_to_parquet(lf.clone(), &raw_data_path.join("census.parquet")).await?;
        fs::create_dir_all(&large_data_path).await?;

        let lf = shorten_census_column_names(lf);
        let lf = expand_census_data(lf, 100).await?;

        write_lf_to_csv(lf.clone(), &large_data_path.join("census.csv")).await?;
        write_lf_to_parquet(lf.clone(), &large_data_path.join("census.parquet")).await?;
        lf
    } else {
        println!("Large census parquet file found - loading...");
        try_read_parquet_to_lf(&large_census_parquet_path)?
    };

    let large_lf = lf.clone();
    let large_data_head =
        tokio::task::spawn_blocking(move || large_lf.limit(5).collect()).await??;
    println!("Head after expansion: {:?}", large_data_head);

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

    // There should be 60_435_100 rows in the census table so
    // if there are fewer than that or the rebuild DB flag has
    let census_count = get_table_count_if_exists("census", &pool).await?;
    if args.rebuild_db || census_count < 60_435_100 {
        debug!("Dropping census table.");
        drop_table_if_exists(&pool, "census").await?;

        println!("Loading CSV file into postgres...");
        load_lf_dynamic(&pool, lf.clone(), "census").await?;
    } else {
        println!("{} rows in census table.", census_count);
    }

    let s3_config = S3Config {
        region: "eu-west-1".into(),
        url: "http://127.0.0.1:9000".into(),
        username: "minioadmin".into(),
        password: "minioadmin".into(),
    };

    let s3_client = S3Client::new(s3_config, "census".into());
    if !s3_client.bucket_exists().await? {
        s3_client.create_bucket().await?;
    };

    let object_exists = s3_client.object_exists("large/census.parquet").await?;
    if !object_exists {
        s3_client
            .stream_chunked_parquet_to_s3(
                large_census_parquet_path.to_str().unwrap(),
                "large/census.parquet",
            )
            .await?;
    }
    let bucket_objects = s3_client.list_objects().await?;
    println!(
        "Found these objects in the {} bucket:\n{:#?}",
        s3_client.get_bucket_name(),
        &bucket_objects
    );

    Ok(())
}
