use anyhow::Result;
use polars::prelude::*;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

async fn synthesise_census_data(lf: LazyFrame) -> Result<LazyFrame> {
    // Start up seeded RNG
    let mut rng = ChaCha8Rng::seed_from_u64(1949);

    let lf_for_height = lf.clone();
    let height = tokio::task::spawn_blocking(move || lf_for_height.collect()).await??;
    let height = height.height();

    // Create random income vector
    let random_income: Vec<i64> = (0..height)
        .map(|_| rng.random_range(10_000..=100_000))
        .collect();

    // Create random weight vector
    let random_weight: Vec<i64> = (0..height).map(|_| rng.random_range(75..=125)).collect();

    let income = Series::new("income".into(), random_income);
    let weight = Series::new("weight".into(), random_weight);

    let lf = lf
        .with_columns([income.lit(), weight.lit()])
        .with_columns([when(col("econ").is_in(
            lit(Series::from_iter(vec![-8, 5, 6, 7, 8, 9])).implode(),
            false,
        ))
        .then(Null {}.lit())
        .otherwise(col("income"))
        .alias("income")]);
    Ok(lf)
}

fn duplicate_lazyframe(lf: LazyFrame, times: usize) -> Result<LazyFrame> {
    let duped_lf = concat(
        vec![lf.clone(); times],
        UnionArgs {
            parallel: true,
            ..Default::default()
        },
    )?;
    Ok(duped_lf)
}

pub(crate) async fn expand_census_data(lf: LazyFrame, times: usize) -> Result<LazyFrame> {
    let lf = synthesise_census_data(lf).await?;
    let lf = duplicate_lazyframe(lf, times)?;
    Ok(lf)
}
