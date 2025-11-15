use clap::Parser;
use std::{env, path};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub(crate) struct Args {
    /// Project root absolute path (defaults to current working directory)
    #[arg(long)]
    project_root: Option<path::PathBuf>,

    /// Data relative path
    #[arg(long, default_value = "data")]
    data_relative_path: path::PathBuf,

    /// Raw data folder name
    #[arg(long, default_value = "raw")]
    raw_data_folder_name: path::PathBuf,

    /// Rebuild postgres DB
    #[arg(long, default_value_t = false)]
    pub(crate) rebuild_db: bool,

    /// Delete bucket
    #[arg(long, default_value_t = false)]
    pub(crate) delete_bucket: bool,
}

impl Args {
    fn get_project_root(&self) -> path::PathBuf {
        self.project_root
            .clone()
            .unwrap_or_else(|| env::current_dir().expect("Failed to get current working directory"))
    }

    pub(crate) fn get_data_path(&self) -> path::PathBuf {
        self.get_project_root().join(&self.data_relative_path)
    }

    pub(crate) fn get_raw_data_path(&self) -> path::PathBuf {
        self.get_data_path().join(&self.raw_data_folder_name)
    }
}
