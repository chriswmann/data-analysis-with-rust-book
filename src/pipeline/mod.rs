pub mod context;
pub mod stage;
pub mod stages;

use crate::pipeline::context::Context;
use crate::pipeline::stage::Stage;
use anyhow::Result;

pub struct Pipeline {
    stages: Vec<Box<dyn Stage>>,
}

impl Pipeline {
    pub async fn new() -> Result<Self> {
        let stages: Vec<Box<dyn Stage>> = vec![
            Box::new(stages::ensure_raw::LoadRawData),
            Box::new(stages::expand_dataset::ExpandDataset),
            Box::new(stages::persist_postgres::PersistPostgres),
            Box::new(stages::persist_s3::PersistS3),
        ];
        Ok(Self { stages })
    }

    pub fn _with_stage(mut self, stage: Box<dyn Stage>) -> Self {
        self.stages.push(stage);
        self
    }

    pub async fn run(self, mut ctx: Context) -> Result<Context> {
        for stage in self.stages {
            ctx = stage.run(ctx).await?;
        }
        Ok(ctx)
    }
}
