use crate::data::rdbms::{get_table_count_if_exists, load_lf_dynamic};
use crate::pipeline::{Stage, context::Context};

use anyhow::Result;
use tracing::info;

pub struct PersistPostgres;

#[async_trait::async_trait]
impl Stage for PersistPostgres {
    fn name(&self) -> &'static str {
        "persist_postgres"
    }

    async fn run(self: Box<Self>, ctx: Context) -> Result<Context> {
        println!("Running stage {}...", self.name());
        let pool = ctx
            .db_pool
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Database pool not found"))?;
        let census_count = get_table_count_if_exists("census", &pool).await?;
        if census_count == 0 {
            info!("Census table is missing or empty");
            let lf = ctx
                .expanded_frame
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Expanded frame not found"))?;
            load_lf_dynamic(&pool, lf, "census").await?;
        } else {
            info!("Census table exists and has {} rows in it.", census_count);
        }
        Ok(ctx)
    }
}
