## Data Analysis in Rust Book

Code for working through the [Data Analysis in Rust Book](https://ericfecteau.ca/data/rust-data-analysis/index.html).
This binary downloads an ONS census teaching dataset, expands it, loads it into Postgres, and uploads artefacts to MinIO.

I wrote this code to learn more rust, rather than to learn data analysis. As such, it's over engineered, with builder patterns used once a configurable pipeline that's only used in one configuration and various bits of functionality that aren't used anywhere (yet), for example.

### Start dependencies (Postgres and MinIO)

From the project root:

```bash
docker compose up -d
```

This starts:

- **Postgres** on `localhost:6543`, database `dair`, user `postgres` / password `postgres`.
- **MinIO** on `http://localhost:9000` with console on `http://localhost:9001` (user `minioadmin` / password `minioadmin`).

### Run the pipeline

From the project root:

```bash
cargo run -- [OPTIONS]
```

On first run this will:

- Download the ONS micro census teaching sample.
- Write raw CSV and parquet files under the configured data directory.
- Expand the dataset to a larger parquet and CSV.
- Load the expanded data into Postgres.
- Upload the large parquet file to the `census` bucket in MinIO.

### CLI arguments

All arguments are optional; defaults are shown in brackets.

- **`--project-root <PATH>`**: Project root absolute path. Defaults to the current working directory.
- **`--data-relative-path <PATH>`**: Data directory relative to the project root. Defaults to `data`.
- **`--raw-data-folder-name <PATH>`**: Subdirectory for raw data. Defaults to `raw`.
- **`--rebuild-db`**: If set, drops and reloads the `census` table in Postgres even if it already contains data.
- **`--delete-bucket`**: If set, deletes and recreates the `census` bucket in MinIO before uploading.

Example with a custom data directory and forced DB rebuild:

```bash
cargo run -- --data-relative-path my-data --rebuild-db
```

### License

This code is licensed under the [Unlicense](./LICENSE-UNLICENSE) or [MIT](./LICENSE-MIT) licenses, at your option.

### Patched connector-x

The code includes a local copy of connector-x that has been patched to use polars 0.52, to avoid issues with incompatible polars dependency versions.
