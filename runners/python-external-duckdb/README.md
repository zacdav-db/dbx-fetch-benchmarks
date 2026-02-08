# python-external-duckdb

## Summary
Benchmarks Unity Catalog external access through DuckDB (`unity_catalog` + `delta`) and returns Arrow tables.

## Setup
1. Install Python dependencies:

```bash
uv sync
```

2. Prepare external clients and shared benchmark assets:

```bash
uv run python scripts/setup_external_clients.py
```

## Environment
Required:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `BENCHMARK_REPEATS`
- `DUCKDB_UC_CATALOG`
- `DUCKDB_UC_SCHEMA`

Optional:
- `DUCKDB_UC_REGION` (defaults to metastore region from SDK)

Behavior:
- Attaches `<DUCKDB_UC_CATALOG>` as `uc`
- Rewrites `samples.tpcds_sf1000.catalog_sales` to `uc.<DUCKDB_UC_SCHEMA>.catalog_sales`

## Run
Run from repo root:

```bash
uv run python runners/python-external-duckdb/run.py
```
