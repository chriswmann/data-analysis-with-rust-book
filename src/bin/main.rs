use anyhow::Result;
use clap::Parser;
use data_analysis_with_rust_book::{Args, run};
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialise structured logging with tracing, letting RUST_LOG control verbosity
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    run(args).await
}
