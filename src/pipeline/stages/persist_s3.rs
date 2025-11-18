use crate::pipeline::{Stage, context::Context};

use anyhow::Result;
use tracing::info;

pub struct PersistS3;

#[async_trait::async_trait]
impl Stage for PersistS3 {
    fn name(&self) -> &'static str {
        "persist_s3"
    }

    async fn run(self: Box<Self>, ctx: Context) -> Result<Context> {
        println!("Running stage {}...", self.name());
        let s3_client = ctx
            .s3_client
            .clone()
            .ok_or_else(|| anyhow::anyhow!("S3 client not found"))?;

        // Ensure the census bucket exists, creating it if necessary
        if !s3_client.bucket_exists().await? {
            s3_client.create_bucket().await?;
        };

        let file_path = ctx.cache_dir.join("large/census.parquet");

        let object_exists = s3_client.object_exists("large/census.parquet").await?;
        if !object_exists {
            info!("census.parquet does not exist in the census bucket. Uploading...");
            s3_client
                .stream_chunked_parquet_to_s3(file_path.to_str().unwrap(), "census")
                .await?;
        } else {
            info!("census.parquet already exists, skipping upload");
        }
        Ok(ctx)
    }
}
