# r-adbc

## Summary
Benchmarks Databricks fetch via ADBC from R (`adbcdrivermanager`) and returns Arrow tables.

## Setup
1. Install R packages:

```r
install.packages(c("adbcdrivermanager", "nanoarrow", "jsonlite", "microbenchmark", "fs", "purrr"))
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
Rscript runners/r-adbc/run.R
```
