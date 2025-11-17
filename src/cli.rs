//! Command-line argument parsing for the data ingestion pipeline.
//!
//! This module defines the flags and path configuration needed to control where data is
//! stored, whether to rebuild existing artefacts, and how to handle bucket lifecycle.
//! All path-related arguments are resolved relative to a configurable project root.

use clap::Parser;
use std::{env, fmt, path};

/// Command-line arguments controlling data paths and rebuild behaviour.
///
/// The CLI supports flexible path configuration (useful when running in containers or CI)
/// and two boolean flags for forcing fresh ingestion into Postgres or MinIO.
#[derive(Clone, Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
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
    /// Returns the configured project root, falling back to the current working directory
    /// if not explicitly provided via `--project-root`.
    fn get_project_root(&self) -> path::PathBuf {
        self.project_root
            .clone()
            .unwrap_or_else(|| env::current_dir().expect("Failed to get current working directory"))
    }

    /// Resolves the data directory by joining the data relative path onto the project root.
    ///
    /// This directory will contain both `raw/` and `large/` subdirectories for the census
    /// datasets at different stages of processing.
    pub(crate) fn get_data_path(&self) -> path::PathBuf {
        self.get_project_root().join(&self.data_relative_path)
    }

    /// Resolves the raw data directory where the original ONS CSV and intermediate artefacts
    /// are stored before expansion.
    pub(crate) fn get_raw_data_path(&self) -> path::PathBuf {
        self.get_data_path().join(&self.raw_data_folder_name)
    }
}

impl fmt::Display for Args {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:?}, {:?}, {:?}, {}, {}",
            self.project_root
                .as_ref()
                .map_or("None".to_string(), |p| format!("{:?}", p)),
            self.data_relative_path,
            self.raw_data_folder_name,
            self.rebuild_db,
            self.delete_bucket
        )
    }
}
