# r-odbc

## Summary
Benchmarks Databricks fetch via ODBC from R (`DBI` + `odbc`) and returns DBI data frames.

## Setup
1. Install R packages:

```r
install.packages(c("DBI", "odbc", "jsonlite", "microbenchmark", "fs", "purrr"))
```

2. Install ODBC/JDBC drivers and print env var paths:

```bash
bash scripts/download_sql_drivers.sh
```

## Environment
Required:
- `DATABRICKS_HOST`
- `DATABRICKS_WAREHOUSE_ID`
- `BENCHMARK_REPEATS`

Optional:
- `DATABRICKS_TOKEN` (only needed if your ambient auth does not already provide credentials)

## Run
Run from repo root:

```bash
Rscript runners/r-odbc/run.R
```
