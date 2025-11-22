use crate::pipeline::{Context, Stage};

use anyhow::Result;
use tracing::info;

#[derive(Debug)]
pub(crate) struct LoadFromS3;

#[async_trait::async_trait]
impl Stage for LoadFromS3 {
    fn name(&self) -> &'static str {
        "load_from_s3"
    }

    #[tracing::instrument]
    async fn run(self: Box<Self>, mut ctx: Context) -> Result<Context> {
        info!("Running stage {}...", self.name());
        let s3_client = ctx
            .s3_client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("S3 client not provided"))?;

        let bucket_name = ctx.bucket_name.as_deref().unwrap_or("census");

        let expanded_s3_key = ctx.dataset_config.expanded_s3_key();
        let lf = s3_client
            .download_lf_from_s3(
                ctx.s3_config
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("S3 config not provided"))?,
                bucket_name,
                expanded_s3_key.as_str(),
            )
            .await?;
        ctx.expanded_frame = Some(lf);
        Ok(ctx)
    }
}
