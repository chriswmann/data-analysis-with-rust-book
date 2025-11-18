use crate::data::{
    etl::{
        shorten_census_column_names, try_read_parquet_to_lf, write_lf_to_csv, write_lf_to_parquet,
    },
    synthesise::expand_census_data,
};
use crate::pipeline::{Stage, context::Context};

use anyhow::Result;
use tokio::fs;
pub struct ExpandDataset;

#[async_trait::async_trait]
impl Stage for ExpandDataset {
    fn name(&self) -> &'static str {
        "expand_dataset"
    }

    async fn run(self: Box<Self>, mut ctx: Context) -> Result<Context> {
        println!("Running stage {}...", self.name());
        let large_data_path = &ctx.cache_dir.join("large");
        let large_census_parquet_path = large_data_path.join("census.parquet");
        let lf = if !large_census_parquet_path.exists() {
            fs::create_dir_all(&large_data_path).await?;

            // Abbreviate column names and synthetically expand to ~60 million rows
            let raw_lf = ctx
                .raw_frame
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Raw frame not found"))?;
            let lf = shorten_census_column_names(raw_lf);
            let lf = expand_census_data(lf, 100).await?;

            // Write the expanded dataset to disk for future runs
            write_lf_to_csv(lf.clone(), &large_data_path.join("census.csv")).await?;
            write_lf_to_parquet(lf.clone(), &large_data_path.join("census.parquet")).await?;
            lf
        } else {
            // Fast path: rehydrate the expanded frame from disk
            println!("Large census parquet file found - loading...");
            try_read_parquet_to_lf(&large_census_parquet_path)?
        };
        // Preview the expanded data to confirm the transformation succeeded
        let large_lf = lf.clone();
        let large_data_head =
            tokio::task::spawn_blocking(move || large_lf.limit(5).collect()).await??;
        println!("Head after expansion: {:?}", large_data_head);

        ctx.expanded_frame = Some(lf);
        Ok(ctx)
    }
}
