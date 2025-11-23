pub mod ensure_raw;
pub mod expand_dataset;
pub mod filters;
pub mod load_from_postgres;
pub mod load_from_s3;
pub mod persist_postgres;
pub mod persist_s3;

#[derive(Clone, Debug)]
pub struct DatasetConfig {
    pub(crate) expanded_dir_name: String,
    pub(crate) filename: String,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        Self {
            expanded_dir_name: "large".into(),
            filename: "census.parquet".into(),
        }
    }
}

impl DatasetConfig {
    /// Returns the S3 key for the expanded dataset.
    pub(crate) fn expanded_s3_key(&self) -> String {
        format!("{}/{}", self.expanded_dir_name, self.filename)
    }
}
