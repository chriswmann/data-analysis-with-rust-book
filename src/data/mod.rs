pub(crate) mod blob;
pub(crate) mod etl;
pub(crate) mod rdbms;
pub(crate) mod synthesise;

#[derive(Clone, Debug)]
pub(crate) enum DataStore {
    Postgres,
    S3,
}
