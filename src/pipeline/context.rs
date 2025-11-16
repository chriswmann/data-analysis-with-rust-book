use std::path;

use anyhow::Result;
use polars::prelude::LazyFrame;
use sqlx::{Pool, Postgres};

use crate::cli::Args;
use crate::data::blob::S3Client;

pub struct Context {
    pub cli: Args,
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
            cli: args.clone(),
            raw_frame: None,
            expanded_frame: None,
            db_pool: None,
            s3_client: None,
            cache_dir,
        })
    }
}
