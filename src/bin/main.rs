use anyhow::Result;
use clap::Parser;
use data_analysis_with_rust_book::{Args, run};

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    run(args).await
}
