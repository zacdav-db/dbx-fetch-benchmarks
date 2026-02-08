# r-brickster-sql

## Summary
Benchmarks Databricks fetch via `{brickster}` SQL execution API and returns Arrow tables.

## Setup
Install R packages:

```r
install.packages(c("brickster", "DBI", "jsonlite", "microbenchmark", "fs", "purrr"))
```

## Environment
Required:
- `DATABRICKS_WAREHOUSE_ID`
- `BENCHMARK_REPEATS`

Authentication:
- `{brickster}` uses ambient Databricks auth (host/token are resolved by your configured auth environment).

## Run
Run from repo root:

```bash
Rscript runners/r-brickster-sql/run.R
```
