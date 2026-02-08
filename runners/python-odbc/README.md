# python-odbc

## Summary
Benchmarks Databricks fetch via ODBC (`pyodbc`) and returns pandas DataFrames.

## Setup
1. Install Python dependencies:

```bash
uv sync
```

2. Install ODBC/JDBC drivers and print env var paths:

```bash
bash scripts/download_sql_drivers.sh
```

## Environment
Required:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_WAREHOUSE_ID`
- `DATABRICKS_ODBC_DRIVER`
- `BENCHMARK_REPEATS`

## Run
Run from repo root:

```bash
uv run python runners/python-odbc/run.py
```
