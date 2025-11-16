use crate::data::synthesise::expand_census_data;
use crate::pipeline::{Stage, context::Context};

use anyhow::Result;

pub struct ExpandDataset;

#[async_trait::async_trait]
impl Stage for ExpandDataset {
    fn name(&self) -> &'static str {
        "expand_dataset"
    }

    async fn run(self: Box<Self>, mut ctx: Context) -> Result<Context> {
        let lf = expand_census_data(
            ctx.raw_frame
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Raw frame not found"))?,
            100,
        )
        .await?;
        ctx.expanded_frame = Some(lf);
        Ok(ctx)
    }
}
