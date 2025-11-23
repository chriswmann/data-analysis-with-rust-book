use std::path;

use anyhow::Result;
use polars::prelude::LazyFrame;
use sqlx::{Pool, Postgres};

use crate::cli::Args;
use crate::data::blob::{S3Client, S3Config};

use crate::data::rdbms::PostgresConn;
use crate::pipeline::stages::DatasetConfig;

/// The state container that flows through the pipeline.
pub struct Context {
    pub args: Args,
    pub dataset_config: DatasetConfig,
    pub raw_frame: Option<LazyFrame>,
    pub expanded_frame: Option<LazyFrame>,
    pub db_pool: Option<Pool<Postgres>>,
    pub pg_conn: Option<PostgresConn>,
    pub s3_client: Option<S3Client>,
    pub s3_config: Option<S3Config>,
    pub table_name: Option<String>,
    pub bucket_name: Option<String>,
    pub cache_dir: path::PathBuf,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("args", &self.args)
            .field("dataset_config", &self.dataset_config)
            .field("raw_frame", &self.raw_frame.is_some())
            .field("expanded_frame", &self.expanded_frame.is_some())
            .field("db_pool", &self.db_pool)
            .field("pg_conn", &self.pg_conn.is_some())
            .field("s3_client", &self.s3_client)
            .field("s3_config", &self.s3_config)
            .field("table_name", &self.table_name)
            .field("bucket_name", &self.bucket_name)
            .field("cache_dir", &self.cache_dir)
            .finish()
    }
}

impl Context {
    pub fn _from_args(args: &Args, dataset_config: DatasetConfig) -> Result<Self> {
        let cache_dir = args.get_data_path();
        Ok(Self {
            args: args.clone(),
            dataset_config,
            raw_frame: None,
            expanded_frame: None,
            db_pool: None,
            pg_conn: None,
            s3_client: None,
            s3_config: None,
            table_name: None,
            bucket_name: None,
            cache_dir,
        })
    }

    pub fn builder(args: &Args, dataset_config: DatasetConfig) -> ContextBuilder {
        ContextBuilder::new(args.clone(), dataset_config)
    }

    pub fn raw_dir(&self) -> path::PathBuf {
        self.cache_dir.join("raw")
    }

    pub fn large_dir(&self) -> path::PathBuf {
        self.cache_dir.join("large")
    }
}

// Custom display to print schemas of the contained LazyFrames without dumping the whole thing.
impl std::fmt::Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let raw_summary = self
            .raw_frame
            .as_ref()
            .map(|lf| {
                format!(
                    "LazyFrame schema: {:?}",
                    lf.clone().collect_schema().unwrap()
                )
            })
            .unwrap_or_else(|| "None".into());

        let expanded_summary = self
            .expanded_frame
            .as_ref()
            .map(|lf| {
                format!(
                    "LazyFrame schema: {:?}",
                    lf.clone().collect_schema().unwrap()
                )
            })
            .unwrap_or_else(|| "None".into());

        write!(
            f,
            "Args: {:?}, Raw Frame: {}, Expanded Frame: {}, DB Pool: {:?}, Cache Dir: {}",
            self.args,
            raw_summary,
            expanded_summary,
            self.db_pool,
            self.cache_dir.to_str().unwrap(),
        )
    }
}

/// Builder to piece together the context, necessary given the number of optional connections (S3, Postgres)
/// and configuration.
pub struct ContextBuilder {
    args: Args,
    dataset_config: DatasetConfig,
    raw_frame: Option<LazyFrame>,
    expanded_frame: Option<LazyFrame>,
    db_pool: Option<Pool<Postgres>>,
    pg_conn: Option<PostgresConn>,
    s3_client: Option<S3Client>,
    s3_config: Option<S3Config>,
    table_name: Option<String>,
    bucket_name: Option<String>,
    cache_dir: Option<path::PathBuf>,
}

impl ContextBuilder {
    pub fn new(args: Args, dataset_config: DatasetConfig) -> Self {
        Self {
            args: args.clone(),
            dataset_config,
            raw_frame: None,
            expanded_frame: None,
            db_pool: None,
            pg_conn: None,
            s3_client: None,
            s3_config: None,
            table_name: None,
            bucket_name: None,
            cache_dir: Some(args.get_data_path()),
        }
    }

    pub fn _with_raw_frame(mut self, raw_frame: LazyFrame) -> Self {
        self.raw_frame = Some(raw_frame);
        self
    }

    pub fn _with_expanded_frame(mut self, expanded_frame: LazyFrame) -> Self {
        self.expanded_frame = Some(expanded_frame);
        self
    }

    pub fn with_db_pool(mut self, db_pool: Pool<Postgres>) -> Self {
        self.db_pool = Some(db_pool);
        self
    }

    pub fn with_pg_conn(mut self, pg_conn: PostgresConn) -> Self {
        self.pg_conn = Some(pg_conn);
        self
    }

    pub fn with_s3_client(mut self, s3_client: S3Client) -> Self {
        self.s3_client = Some(s3_client);
        self
    }

    pub fn with_s3_config(mut self, s3_config: S3Config) -> Self {
        self.s3_config = Some(s3_config);
        self
    }

    pub fn with_table_name(mut self, table_name: String) -> Self {
        self.table_name = Some(table_name);
        self
    }

    pub fn with_bucket_name(mut self, bucket_name: String) -> Self {
        self.bucket_name = Some(bucket_name);
        self
    }
    pub fn with_cache_dir(mut self, cache_dir: path::PathBuf) -> Self {
        self.cache_dir = Some(cache_dir);
        self
    }

    pub fn build(self) -> Result<Context> {
        // Ensure we have a valid cache directory before finalising.
        let cache_dir = self
            .cache_dir
            .ok_or_else(|| anyhow::anyhow!("Cache directory must be provided "))?;

        Ok(Context {
            args: self.args,
            dataset_config: self.dataset_config,
            raw_frame: self.raw_frame,
            expanded_frame: self.expanded_frame,
            db_pool: self.db_pool,
            pg_conn: self.pg_conn,
            s3_client: self.s3_client,
            s3_config: self.s3_config,
            table_name: self.table_name,
            bucket_name: self.bucket_name,
            cache_dir,
        })
    }
}
