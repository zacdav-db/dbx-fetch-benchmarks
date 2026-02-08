# python-jdbc

## Summary
Benchmarks Databricks fetch via JDBC (`jaydebeapi` + Databricks JDBC driver) and returns pandas DataFrames.

## Setup
1. Install Java (JDK/JRE, Java 11+).
2. Install Python dependencies:

```bash
uv sync
```

3. Download drivers and print env var paths:

```bash
bash scripts/download_sql_drivers.sh
```

## Environment
Required:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_WAREHOUSE_ID`
- `JDBC_JAR_PATH`
- `BENCHMARK_REPEATS`

Note:
- Runner starts JVM with Arrow-compatible `--add-opens` for Databricks JDBC Arrow fetch path.

## Run
Run from repo root:

```bash
uv run python runners/python-jdbc/run.py
```
