use crate::pipeline::{Context, stage::Stage};

use anyhow::Result;
use polars::prelude::*;
use tracing::info;

#[derive(Debug)]
pub struct SimpleDataFilter;

#[async_trait::async_trait]
impl Stage for SimpleDataFilter {
    fn name(&self) -> &'static str {
        "simple_filter"
    }

    #[tracing::instrument]
    async fn run(self: Box<Self>, mut ctx: Context) -> Result<Context> {
        let lf = ctx
            .expanded_frame
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Expanded dataframe not in context for filter stage."))?
            .clone();
        let lf_filt_one = lf.clone().filter(
            col("keep_type")
                .eq(lit(1)) // Usual resident
                .and(col("region").eq(lit("E12000007"))) // London
                .and(col("age_group").eq(lit(5))) // Aged 45+
                .and(col("income").is_not_null()),
        );
        ctx.simple_filter_frame = Some(lf_filt_one.clone());
        let lf_filt_one_for_info = lf_filt_one.clone();
        let df_head =
            tokio::task::spawn_blocking(|| lf_filt_one_for_info.limit(5).collect().unwrap())
                .await?;
        info!("Simple filter frame: {:?}", df_head,);
        Ok(ctx)
    }
}

#[derive(Debug)]
pub struct ComplexDataFilter;

#[async_trait::async_trait]
impl Stage for ComplexDataFilter {
    fn name(&self) -> &'static str {
        "complex_filter"
    }

    #[tracing::instrument]
    async fn run(self: Box<Self>, mut ctx: Context) -> Result<Context> {
        info!("Running stage {}...", self.name());
        // ((region == "E12000001" & age_group >= 6) | (region == "E12000002" & age_group < 6))
        let expr = (col("region")
            .eq(lit("E12000001")) // North East
            .and(col("age_group").gt_eq(lit(6)))) // 55 and over
        .or(col("region")
            .eq(lit("E12000002")) // North West
            .and(col("age_group").lt_eq(lit(6)))); // 54 and under

        println!("{expr}"); // You can print it

        let lf = ctx
            .expanded_frame
            .as_mut()
            .ok_or_else(|| {
                anyhow::anyhow!("Expanded frame not in context for complex filter stage.")
            })?
            .clone();
        let lf_filt_two = lf.filter(expr);
        ctx.complex_filter_frame = Some(lf_filt_two.clone());
        let lf_filt_two_for_info = lf_filt_two.clone();
        let df_head =
            tokio::task::spawn_blocking(|| lf_filt_two_for_info.limit(5).collect().unwrap())
                .await?;
        info!("Complex filter frame: {:?}", df_head,);
        Ok(ctx)
    }
}
