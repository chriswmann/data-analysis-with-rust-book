use std::io::Cursor;
use std::path;

use anyhow::Result;
use polars::prelude::*;
use tokio::{self, fs};
use tracing::debug;

/// Loads a CSV file from a URL into a Polars LazyFrame.
///
/// This function downloads CSV data from a remote URL via HTTP(S), buffers it entirely
/// in memory, then parses it into a Polars DataFrame and returns it as a LazyFrame for
/// efficient lazy evaluation.
///
/// # Arguments
///
/// * `url` - A string slice containing the HTTP(S) URL of the CSV file to download
///
/// # Returns
///
/// * `Result<LazyFrame>` - A LazyFrame containing the parsed CSV data, or an error if:
///   - The HTTP request fails (network error, invalid URL, 4xx/5xx status codes)
///   - The response body cannot be read into memory
///   - The CSV parsing fails (malformed CSV, encoding issues)
///
/// # Implementation Notes
///
/// - **Memory usage**: The entire CSV file is downloaded into memory before parsing.
///   For very large files (multiple GB), this may cause high memory consumption.
/// - **No size limit**: Unlike the default ureq behavior, this implementation uses
///   `read_to_vec()` which bypasses ureq's 10MB response size limit, allowing
///   downloads of arbitrarily large files (limited only by available RAM).
/// - **Why Cursor?**: Polars' `CsvReader` requires types implementing `MmapBytesReader`.
///   Since ureq's `Body` doesn't implement this trait, we buffer the data into a
///   `Vec<u8>` and wrap it in a `Cursor`, which does implement the required trait.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// # fn main() -> Result<()> {
/// let lf = load_csv_from_url("https://example.com/data.csv")?;
/// let df = lf.collect()?; // Evaluate the lazy operations
/// println!("{}", df);
/// # Ok(())
/// # }
/// ```
#[tracing::instrument]
pub(crate) async fn load_csv_from_url(url: &str) -> Result<LazyFrame> {
    // Make an HTTP GET request to download the CSV file
    let response = reqwest::get(url).await?.text().await?;

    // Read the entire response body into a byte vector
    // Note: read_to_vec() bypasses ureq's default 10MB size limit
    let bytes = response.as_bytes();

    // Wrap the byte vector in a Cursor to provide the MmapBytesReader trait
    // required by Polars' CsvReader
    let cursor = Cursor::new(bytes);

    // Parse the CSV data into an eager DataFrame
    let data = CsvReader::new(cursor).finish()?;

    // Convert to a LazyFrame for efficient query optimisation
    Ok(data.lazy())
}

/// Try to load a file locally in case it's already been downloaded.
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

#[tracing::instrument(skip(lf))]
pub(crate) async fn write_lf_to_csv(lf: LazyFrame, file_path: &path::Path) -> Result<()> {
    let file = fs::File::create(file_path).await?;
    let mut file = file.into_std().await;
    let mut df = tokio::task::spawn_blocking(move || lf.clone().collect()).await??;

    CsvWriter::new(&mut file).finish(&mut df)?;
    println!("File saved to {}", file_path.display());
    Ok(())
}

/// Try to load a file locally in case it's already been downloaded.
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

#[tracing::instrument(skip(lf))]
pub(crate) async fn write_lf_to_parquet(lf: LazyFrame, file_path: &path::Path) -> Result<()> {
    let file = fs::File::create(file_path).await?;
    let mut file = file.into_std().await;
    let mut df = tokio::task::spawn_blocking(move || lf.clone().collect()).await??;

    ParquetWriter::new(&mut file).finish(&mut df)?;
    println!("File saved to {}", file_path.display());
    Ok(())
}

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
            let lf = load_csv_from_url(url).await?;
            write_lf_to_csv(lf.clone(), file_path).await?;
            Ok(lf)
        }
    }
}

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
