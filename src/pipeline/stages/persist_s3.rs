use crate::pipeline::{Stage, context::Context};

use anyhow::Result;

pub struct PersistS3;

#[async_trait::async_trait]
impl Stage for PersistS3 {
    fn name(&self) -> &'static str {
        "persist_s3"
    }

    async fn run(self: Box<Self>, ctx: Context) -> Result<Context> {
        let s3_client = ctx
            .s3_client
            .clone()
            .ok_or_else(|| anyhow::anyhow!("S3 client not found"))?;
        let file_path = ctx.cache_dir.join("large/census.parquet");
        s3_client
            .stream_chunked_parquet_to_s3(file_path.to_str().unwrap(), "census")
            .await?;
        Ok(ctx)
    }
}
