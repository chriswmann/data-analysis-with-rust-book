use crate::pipeline::context::Context;
use anyhow::Result;

#[async_trait::async_trait]
pub trait Stage {
    fn name(&self) -> &'static str;
    async fn run(self: Box<Self>, ctx: Context) -> Result<Context>;
}
