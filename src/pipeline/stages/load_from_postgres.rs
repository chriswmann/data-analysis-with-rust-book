use crate::data::rdbms::load_lf_from_postgres;
use crate::pipeline::{Context, Stage};

use anyhow::Result;
use tracing::info;

#[derive(Debug)]
pub(crate) struct LoadFromPostgres;

#[async_trait::async_trait]
impl Stage for LoadFromPostgres {
    fn name(&self) -> &'static str {
        "load_from_postgres"
    }

    #[tracing::instrument]
    async fn run(self: Box<Self>, mut ctx: Context) -> Result<Context> {
        info!("Running stage {}...", self.name());
        let pg_conn = ctx
            .pg_conn
            .as_ref()
            .expect("Postgres connection not provided");
        let lf = load_lf_from_postgres("census", pg_conn).await?;
        ctx.expanded_frame = Some(lf);
        Ok(ctx)
    }
}
