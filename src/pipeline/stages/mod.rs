pub mod ensure_raw;
pub mod expand_dataset;
pub mod load_from_postgres;
pub mod load_from_s3;
pub mod persist_postgres;
pub mod persist_s3;

#[derive(Clone, Debug)]
pub(crate) struct DatasetConfig {
    pub(crate) bucket_name: String,
    pub(crate) table_name: String,
    pub(crate) raw_dir_name: String,
    pub(crate) expanded_dir_name: String,
    pub(crate) filename: String,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        Self {
            bucket_name: "census".into(),
            table_name: "census".into(),
            raw_dir_name: "raw".into(),
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

    /// Returns the S3 key for the raw dataset.
    pub(crate) fn raw_parquet_key(&self) -> String {
        format!("{}/{}", self.raw_dir_name, self.filename)
    }
}
