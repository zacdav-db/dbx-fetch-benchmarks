# python-adbc

## Summary
Benchmarks Databricks fetch via ADBC (`adbc_driver_manager`) and returns Arrow tables.

## Setup
1. Install Python dependencies:

```bash
uv sync
```

2. Install the Databricks ADBC driver:

```bash
dbc install databricks
```

## Environment
Required:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_WAREHOUSE_ID`
- `BENCHMARK_REPEATS`

## Run
Run from repo root:

```bash
uv run python runners/python-adbc/run.py
```
