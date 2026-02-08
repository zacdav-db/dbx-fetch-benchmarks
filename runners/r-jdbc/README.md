# r-jdbc

## Summary
Benchmarks Databricks fetch via JDBC from R (`DBI` + `RJDBC`) and returns DBI data frames.

## Setup
1. Install Java (JDK/JRE, Java 11+).
2. Install R packages:

```r
install.packages(c("DBI", "RJDBC", "jsonlite", "microbenchmark", "fs", "purrr"))
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
- The runner sets Arrow-compatible Java `--add-opens` options required by current Databricks JDBC Arrow path.

## Run
Run from repo root:

```bash
Rscript runners/r-jdbc/run.R
```
