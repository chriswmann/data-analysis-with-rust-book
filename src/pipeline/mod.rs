pub mod context;
pub mod stage;
pub mod stages;

use crate::cli::Args;
use crate::data::DataStore;
use crate::errors::{PipelineBuildError, PipelineValidationError};
use crate::pipeline::stages::{
    ensure_raw::LoadRawData, expand_dataset::ExpandDataset, load_from_postgres::LoadFromPostgres,
    load_from_s3::LoadFromS3, persist_postgres::PersistPostgres, persist_s3::PersistS3,
};
use crate::pipeline::{context::Context, stage::Stage};
use anyhow::Result;

pub struct Pipeline {
    stages: Vec<Box<dyn Stage>>,
}

impl Pipeline {
    pub fn builder(args: &Args) -> PipelineBuilder {
        PipelineBuilder::new(args)
    }

    pub async fn run(self, mut ctx: Context) -> Result<Context> {
        for stage in self.stages {
            ctx = stage.run(ctx).await?;
        }
        Ok(ctx)
    }
}

pub(crate) struct PipelineBuilder {
    args: Args,
    ensure_raw_stage: LoadRawData,
    expand_stage: ExpandDataset,
    persist_pg_stage: Option<PersistPostgres>,
    persist_s3_stage: Option<PersistS3>,
    load_pg_stage: Option<LoadFromPostgres>,
    load_s3_stage: Option<LoadFromS3>,
}

impl PipelineBuilder {
    pub fn new(args: &Args) -> Self {
        PipelineBuilder {
            args: args.clone(),
            ensure_raw_stage: LoadRawData,
            expand_stage: ExpandDataset,
            persist_pg_stage: None,
            persist_s3_stage: None,
            load_pg_stage: None,
            load_s3_stage: None,
        }
    }

    pub fn with_persistence(mut self, destination: DataStore) -> Self {
        match destination {
            DataStore::Postgres => self.persist_pg_stage = Some(PersistPostgres),
            DataStore::S3 => self.persist_s3_stage = Some(PersistS3),
        }
        self
    }

    pub fn with_data_source(mut self, source: DataStore) -> Self {
        match source {
            DataStore::Postgres => self.load_pg_stage = Some(LoadFromPostgres),
            DataStore::S3 => self.load_s3_stage = Some(LoadFromS3),
        }

        self
    }

    fn validate(&self) -> Result<()> {
        let mut errors = Vec::new();
        let at_least_one_persist_stage =
            self.persist_pg_stage.is_some() || self.persist_s3_stage.is_some();
        if !at_least_one_persist_stage {
            errors.push(PipelineBuildError::AtLeastOnePersistStageMustBeSpecified);
        }
        let persist_and_load_stores_are_the_same = self.persist_pg_stage.is_some()
            && self.load_pg_stage.is_some()
            || self.persist_s3_stage.is_some() && self.load_s3_stage.is_some();
        if !persist_and_load_stores_are_the_same {
            errors.push(PipelineBuildError::PersistAndLoadStoresMustBeTheSame);
        }

        let args_specified_load_store_is_available = self.args.load.use_s3_data
            && self.load_s3_stage.is_some()
            || self.args.load.use_postgres_data && self.load_pg_stage.is_some();
        if !args_specified_load_store_is_available {
            let store = if self.args.load.use_s3_data {
                "S3".to_string()
            } else {
                "Postgres".to_string()
            };
            errors.push(PipelineBuildError::ArgsLoadStoreNotAvailable(store));
        }

        if !errors.is_empty() {
            return Err(PipelineValidationError { errors }.into());
        }
        Ok(())
    }

    pub fn build(self) -> Result<Pipeline> {
        self.validate()?;
        let mut stages: Vec<Box<dyn Stage>> = Vec::new();
        stages.push(Box::new(self.ensure_raw_stage));
        stages.push(Box::new(self.expand_stage));
        if self.persist_pg_stage.is_some() {
            stages.push(Box::new(self.persist_pg_stage.unwrap()));
        }
        if self.persist_s3_stage.is_some() && self.args.load.use_s3_data {
            stages.push(Box::new(self.persist_s3_stage.unwrap()));
        }
        if self.load_pg_stage.is_some() {
            stages.push(Box::new(self.load_pg_stage.unwrap()));
        }
        if self.load_s3_stage.is_some() {
            stages.push(Box::new(self.load_s3_stage.unwrap()));
        }
        Ok(Pipeline { stages })
    }
}
