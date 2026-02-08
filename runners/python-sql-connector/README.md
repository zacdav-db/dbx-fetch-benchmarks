# python-sql-connector

## Summary
Benchmarks Databricks SQL Connector for Python (`databricks-sql-connector`) and fetches Arrow results (`fetchall_arrow`).

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
- `BENCHMARK_REPEATS`

## Run
Run from repo root:

```bash
uv run python runners/python-sql-connector/run.py
```
