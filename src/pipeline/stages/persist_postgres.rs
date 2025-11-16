use crate::data::rdbms::load_lf_dynamic;
use crate::pipeline::{Stage, context::Context};

use anyhow::Result;

pub struct PersistPostgres;

#[async_trait::async_trait]
impl Stage for PersistPostgres {
    fn name(&self) -> &'static str {
        "persist_postgres"
    }

    async fn run(self: Box<Self>, ctx: Context) -> Result<Context> {
        let pool = ctx
            .db_pool
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Database pool not found"))?;
        let lf = ctx
            .expanded_frame
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Expanded frame not found"))?;
        load_lf_dynamic(&pool, lf, "census").await?;
        Ok(ctx)
    }
}
