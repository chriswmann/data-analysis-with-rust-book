use crate::data::etl::{RAW_URL, get_data, write_lf_to_parquet};
use crate::pipeline::{Stage, context::Context};

use anyhow::Result;
use tokio::fs;
pub struct LoadRawData;

#[async_trait::async_trait]
impl Stage for LoadRawData {
    fn name(&self) -> &'static str {
        "load_raw_data"
    }

    async fn run(self: Box<Self>, mut ctx: Context) -> Result<Context> {
        println!("Running stage {}...", self.name());
        println!("Ensuring raw census artefacts exist - downloading and processing if needed...");
        let raw_data_path = &ctx.cache_dir.join("raw");
        fs::create_dir_all(&raw_data_path).await?;

        // Download the ONS micro census teaching sample from the public endpoint
        let micro_census_data_url = RAW_URL;
        let lf = get_data(&raw_data_path.join("census.csv"), micro_census_data_url).await?;

        // Preview the raw data before any transformations
        let raw_lf = lf.clone();
        let raw_data_head =
            tokio::task::spawn_blocking(move || raw_lf.limit(5).collect()).await??;
        println!("Head: {:?}", raw_data_head);
        // Persist the original dataset in Parquet format for archival purposes
        write_lf_to_parquet(lf.clone(), &raw_data_path.join("census.parquet")).await?;
        ctx.raw_frame = Some(lf);
        Ok(ctx)
    }
}
