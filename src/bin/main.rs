use anyhow::Result;
use clap::Parser;
use data_analysis_with_rust_book::{Args, run};
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialise structured logging with tracing, letting RUST_LOG control verbosity
    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    run(args).await
}
