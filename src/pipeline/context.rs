use std::path;

use anyhow::Result;
use polars::prelude::LazyFrame;
use sqlx::{Pool, Postgres};

use crate::cli::Args;
use crate::data::blob::S3Client;

pub struct Context {
    pub args: Args,
    pub raw_frame: Option<LazyFrame>,
    pub expanded_frame: Option<LazyFrame>,
    pub db_pool: Option<Pool<Postgres>>,
    pub s3_client: Option<S3Client>,
    pub cache_dir: path::PathBuf,
}

impl Context {
    pub fn from_args(args: &Args) -> Result<Self> {
        let cache_dir = args.get_data_path();
        Ok(Self {
            args: args.clone(),
            raw_frame: None,
            expanded_frame: None,
            db_pool: None,
            s3_client: None,
            cache_dir,
        })
    }

    pub fn builder(args: &Args) -> ContextBuilder {
        ContextBuilder::new(args.clone())
    }
}

impl std::fmt::Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Args: {:?}, Raw Frame: {:?}, Expanded Frame: {:?}, DB Pool: {:?}, Cache Dir: {}",
            self.args,
            self.raw_frame.clone().map_or("None".to_string(), |lf| lf
                .limit(5)
                .collect()
                .unwrap()
                .to_string()),
            self.expanded_frame
                .clone()
                .map_or("None".to_string(), |lf| lf
                    .limit(5)
                    .collect()
                    .unwrap()
                    .to_string()),
            self.db_pool,
            self.cache_dir.to_str().unwrap(),
        )
    }
}

pub struct ContextBuilder {
    args: Args,
    raw_frame: Option<LazyFrame>,
    expanded_frame: Option<LazyFrame>,
    db_pool: Option<Pool<Postgres>>,
    s3_client: Option<S3Client>,
    cache_dir: Option<path::PathBuf>,
}

impl ContextBuilder {
    pub fn new(args: Args) -> Self {
        Self {
            args: args.clone(),
            raw_frame: None,
            expanded_frame: None,
            db_pool: None,
            s3_client: None,
            cache_dir: Some(args.get_data_path()),
        }
    }

    pub fn with_raw_frame(mut self, raw_frame: LazyFrame) -> Self {
        self.raw_frame = Some(raw_frame);
        self
    }

    pub fn with_expanded_frame(mut self, expanded_frame: LazyFrame) -> Self {
        self.expanded_frame = Some(expanded_frame);
        self
    }

    pub fn with_db_pool(mut self, db_pool: Pool<Postgres>) -> Self {
        self.db_pool = Some(db_pool);
        self
    }

    pub fn with_s3_client(mut self, s3_client: S3Client) -> Self {
        self.s3_client = Some(s3_client);
        self
    }

    pub fn with_cache_dir(mut self, cache_dir: path::PathBuf) -> Self {
        self.cache_dir = Some(cache_dir);
        self
    }

    pub fn build(self) -> Result<Context> {
        let cache_dir = self
            .cache_dir
            .ok_or_else(|| anyhow::anyhow!("Cache directory must be provided "))?;

        Ok(Context {
            args: self.args,
            raw_frame: self.raw_frame,
            expanded_frame: self.expanded_frame,
            db_pool: self.db_pool,
            s3_client: self.s3_client,
            cache_dir,
        })
    }
}
