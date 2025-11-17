//! High-level extract and load helpers built for the book's datasets.
//!
//! The routines here focus on two workflows:
//! 1. Fetching CSV assets (either from disk or over HTTP) and presenting them as lazy
//!    Polars plans ready for downstream transforms.
//! 2. Persisting large frames back to disk, keeping async boundaries explicit so callers
//!    can orchestrate ingestion pipelines without blocking.

use std::path;

use anyhow::Result;
use polars::prelude::*;
use tokio::{self, fs};
use tracing::debug;

pub(crate) const RAW_URL: &str = "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/publicmicrodatateachingsampleenglandandwalescensus2021/current/upload-publicmicrodatateachingsample.csv";

/// Downloads a remote CSV and exposes it as a `LazyFrame`.
///
/// The HTTP body is buffered into memory, wrapped in a `Cursor`, then parsed with the
/// eager `CsvReader` before being converted into a lazy plan so the caller can append
/// projections or filters without triggering execution.
#[tracing::instrument]
pub(crate) async fn fetch_and_cache_csv(url: &str, file_path: &path::Path) -> Result<LazyFrame> {
    // Make an HTTP GET request to download the CSV file
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) data-analysis-with-rust-book/0.1",
        )
        .header(
            "Accept",
            "text/csv,application/octet-stream;q=0.9,*/*;q=0.1",
        )
        .send()
        .await?;

    let status = response.status();
    if status != reqwest::StatusCode::OK {
        anyhow::bail!("Failed to download CSV: {}", status);
    }
    // Write to disk first so we can reuse try_read_csv_to_lf (which has infer_schema_length set
    // and which is needed to read the file properly)
    fs::write(file_path, response.bytes().await?).await?;
    let lf = try_read_csv_to_lf(file_path)?;

    // Convert to a LazyFrame for efficient query optimisation
    Ok(lf)
}

/// Attempts to read a `LazyFrame` from an on-disk CSV, failing fast if the path is
/// absent or invalid UTF-8.
#[tracing::instrument]
fn try_read_csv_to_lf(file_path: &path::Path) -> Result<LazyFrame> {
    if !file_path.exists() {
        anyhow::bail!("File not found: {}", file_path.display());
    };

    let lf = LazyCsvReader::new(PlPath::new(
        file_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in file path"))?,
    ))
    .with_infer_schema_length(Some(10_000))
    .with_has_header(true)
    .finish()?;
    Ok(lf)
}

/// Collects a `LazyFrame` on a blocking task and streams the rows to a CSV writer.
///
/// The hand-off to `tokio::spawn_blocking` protects the async runtime from CPU-bound
/// Polars work, while the Tokio file handle keeps the operation fully asynchronous.
#[tracing::instrument(skip(lf))]
pub(crate) async fn write_lf_to_csv(lf: LazyFrame, file_path: &path::Path) -> Result<()> {
    let file = fs::File::create(file_path).await?;
    let mut file = file.into_std().await;
    let mut df = tokio::task::spawn_blocking(move || lf.clone().collect()).await??;

    CsvWriter::new(&mut file).finish(&mut df)?;
    println!("File saved to {}", file_path.display());
    Ok(())
}

/// Lazily scans a parquet file, logging the path for easier traceability when multiple
/// artefacts live side-by-side on disk.
#[tracing::instrument]
pub(crate) fn try_read_parquet_to_lf(file_path: &path::Path) -> Result<LazyFrame> {
    if !file_path.exists() {
        anyhow::bail!("File not found: {}", file_path.display());
    };

    debug!("Opening {:?}", file_path);
    let lf = LazyFrame::scan_parquet(
        PlPath::new(
            file_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in file path"))?,
        ),
        ScanArgsParquet::default(),
    )?;
    Ok(lf)
}

/// Writes a collected `LazyFrame` into parquet, mirroring the CSV helper but targeting
/// columnar storage for downstream analytics.
#[tracing::instrument(skip(lf))]
pub(crate) async fn write_lf_to_parquet(lf: LazyFrame, file_path: &path::Path) -> Result<()> {
    let file = fs::File::create(file_path).await?;
    let mut file = file.into_std().await;
    let mut df = tokio::task::spawn_blocking(move || lf.clone().collect()).await??;

    ParquetWriter::new(&mut file).finish(&mut df)?;
    println!("File saved to {}", file_path.display());
    Ok(())
}

/// Returns a `LazyFrame` sourced from a cached CSV when available, falling back to a
/// remote download that is persisted locally for future runs.
#[tracing::instrument]
pub(crate) async fn get_data(file_path: &path::Path, url: &str) -> Result<LazyFrame> {
    match try_read_csv_to_lf(file_path) {
        Ok(lf) => {
            println!("Local file found and loaded from {}", file_path.display());
            Ok(lf)
        }
        Err(_) => {
            println!(
                "No local file found at {}.\nDownloading from {}.",
                file_path.display(),
                url
            );
            let lf = fetch_and_cache_csv(url, file_path).await?;
            Ok(lf)
        }
    }
}

/// Projects the wide census schema down to the abbreviations used across the chapter,
/// keeping the intent explicit within a single select.
#[tracing::instrument(skip(lf))]
pub(crate) fn shorten_census_column_names(lf: LazyFrame) -> LazyFrame {
    lf.select([
        col("resident_id_m").alias("id"),
        col("approx_social_grade").alias("social"),
        col("country_of_birth_3a").alias("birth"),
        col("economic_activity_status_10m").alias("econ"),
        col("ethnic_group_tb_6a").alias("ethnic"),
        col("health_in_general").alias("health"),
        col("hh_families_type_6a").alias("fam_type"),
        col("hours_per_week_worked").alias("hours_worked"),
        col("in_full_time_education").alias("education"),
        col("industry_10a").alias("industry"),
        col("iol22cd").alias("london"),
        col("legal_partnership_status_6a").alias("mar_stat"),
        col("occupation_10a").alias("occupation"),
        col("region"),
        col("religion_tb").alias("religion"),
        col("residence_type"),
        col("resident_age_7d").alias("age_group"),
        col("sex"),
        col("usual_short_student").alias("keep_type"),
    ])
}
