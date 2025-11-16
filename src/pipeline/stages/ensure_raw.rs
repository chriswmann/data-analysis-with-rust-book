use crate::data::etl::{RAW_URL, get_data};
use crate::pipeline::{Stage, context::Context};

use anyhow::Result;

pub struct LoadRawData;

#[async_trait::async_trait]
impl Stage for LoadRawData {
    fn name(&self) -> &'static str {
        "load_raw_data"
    }

    async fn run(self: Box<Self>, mut ctx: Context) -> Result<Context> {
        let lf = get_data(&ctx.cache_dir.join("census.csv"), RAW_URL).await?;
        ctx.raw_frame = Some(lf);
        Ok(ctx)
    }
}
