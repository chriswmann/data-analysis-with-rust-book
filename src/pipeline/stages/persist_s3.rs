use crate::pipeline::{Stage, context::Context};

use anyhow::Result;
use tracing::info;

#[derive(Debug)]
pub struct PersistS3;

#[async_trait::async_trait]
impl Stage for PersistS3 {
    fn name(&self) -> &'static str {
        "persist_s3"
    }

    #[tracing::instrument]
    async fn run(self: Box<Self>, ctx: Context) -> Result<Context> {
        info!("Running stage {}...", self.name());
        let s3_client = ctx
            .s3_client
            .clone()
            .ok_or_else(|| anyhow::anyhow!("S3 client not found"))?;

        let bucket_name = &ctx
            .bucket_name
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Bucket name not found"))?;
        // Ensure the census bucket exists, creating it if necessary
        if !s3_client.bucket_exists(bucket_name).await? {
            s3_client.create_bucket(bucket_name).await?;
        };

        let file_path = ctx.cache_dir.join("large/census.parquet");

        let object_exists = s3_client
            .object_exists(bucket_name, "large/census.parquet")
            .await?;
        if !object_exists {
            info!("census.parquet does not exist in the census bucket. Uploading...");
            s3_client
                .stream_chunked_parquet_to_s3(
                    file_path.to_str().unwrap(),
                    bucket_name,
                    "large/census.parquet",
                )
                .await?;
        } else {
            info!("census.parquet already exists, skipping upload");
        }
        Ok(ctx)
    }
}
