use anyhow::Result;
use clap::Parser;
use data_analysis_with_rust_book::{
    Args, Context, DatasetConfig, Pipeline, PostgresConn, S3Client, S3Config,
};

#[tokio::main]
async fn main() -> Result<()> {
    let pipeline = Pipeline::builder()
        .with_postgres_persistence()
        .with_s3_persistence()
        .with_postgres_retrieval()
        .finish();

    let postgres_conn = PostgresConn {
        user: "postgres".into(),
        password: "postgres".into(),
        host: "localhost".into(),
        port: 6543,
        database: "dair".into(),
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(20)
        .connect(postgres_conn.get_db_connection_string().as_str())
        .await?;
    let s3_config = S3Config::default();
    let s3_client = S3Client::new(&s3_config);
    let dataset_config = DatasetConfig::default();
    let args = Args::parse();
    let ctx = Context::builder(&args, dataset_config)
        .with_bucket_name("etl-only".into())
        .with_table_name("etl_only".into())
        .with_cache_dir(args.get_data_path())
        .with_db_pool(pool)
        .with_pg_conn(postgres_conn)
        .with_s3_client(s3_client)
        .with_s3_config(s3_config)
        .build()?;

    let ctx = pipeline.run(ctx).await?;
    println!("Context:\n{}", ctx);
    Ok(())
}
