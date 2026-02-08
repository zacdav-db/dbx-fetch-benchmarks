# python-volume-download

## Summary
Control benchmark that materializes each scenario to a UC Volume as Parquet, downloads files locally, and reads Arrow tables.

## Setup
Install Python dependencies:

```bash
uv sync
```

## Environment
Required:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_WAREHOUSE_ID`
- `DATABRICKS_VOLUME_PATH` (for example `/Volumes/<catalog>/<schema>/<volume>/<prefix>`)
- `BENCHMARK_REPEATS`

Behavior:
- Materializes scenario data once before timing
- Uses `REPARTITION(1)` and Parquet `zstd` compression
- Times download + local Arrow load only
- Uses `download_to(..., use_parallel=True, parallelism=10)`
- Cleans temp local files each iteration and clears remote materialized files at end

## Run
Run from repo root:

```bash
uv run python runners/python-volume-download/run.py
```
