# Databricks Fetch Benchmarks

Minimal benchmark suite for Databricks fetch performance across clients/languages.

> [!IMPORTANT]
> This isn't perfect, but hopefully its not way off. Contributions are welcome if theres a way to improve one of the methods.

## Dataset and scenarios

- Target dataset: `samples.tpcds_sf1000.catalog_sales`
- Shared scenario definitions: `queries/scenarios.json`
- Benchmark policy per scenario:
  - `1` warmup run
  - `BENCHMARK_REPEATS` timed repeats (currently `5`)

## Options tested

Arrow-first behavior:
- Runners that support Arrow return Arrow tables.
- Runners without Arrow support return row frames (`pandas` `DataFrames` in Python, `data.frames`/`tibbles` in R).

| Runner | Language | Method | Return type | Extra setup |
| --- | --- | --- | --- | --- |
| `python-sql-connector` | Python | SQL Connector | Arrow Table | None |
| `python-volume-download` | Python | Volume Parquet export + download | Arrow Table | `DATABRICKS_VOLUME_PATH` |
| `python-adbc` | Python | ADBC | Arrow Table | Install `dbc`, then `dbc install databricks` |
| `python-odbc` | Python | ODBC | `pandas` DataFrame | ODBC driver |
| `python-jdbc` | Python | JDBC | `pandas` DataFrame | JDBC JAR + Java |
| `python-sharing-client` | Python | Delta Sharing client | `pandas` DataFrame | `scripts/setup_external_clients.py` |
| `python-sharing-client-hack` | Python | Delta Sharing + `delta_kernel_rust_sharing_wrapper` | Arrow Table | `scripts/setup_external_clients.py` |
| `python-external-duckdb` | Python | DuckDB UC external access (`unity_catalog` + `delta`) | Arrow Table | `scripts/setup_external_clients.py` |
| `r-brickster-sql` | R | Brickster SQL | Arrow Table | None |
| `r-adbc` | R | ADBC | Arrow Table | Install `dbc`, then `dbc install databricks` |
| `r-odbc` | R | ODBC | `DBI` data frame (tibble-compatible) | ODBC driver |
| `r-jdbc` | R | JDBC | `DBI` data frame (tibble-compatible) | JDBC JAR + Java |

Each runner has its own `README.md` under `runners/`.

## Setup

1. Install Python dependencies:

```bash
uv sync
```

2. Install `dbc` CLI:

```bash
uv tool install dbc
```

3. Install Databricks ADBC driver (for `python-adbc` and `r-adbc`):

```bash
dbc install databricks
```

4. Download JDBC/ODBC drivers and print env var values:

```bash
bash scripts/download_sql_drivers.sh
```

Optional (if you want to pass an ODBC package URL directly):

```bash
DATABRICKS_ODBC_DOWNLOAD_URL='<driver-package-url>' bash scripts/download_sql_drivers.sh
```

5. Copy env templates:
- `.env.example` -> `.env`
- `.Renviron.example` -> `.Renviron`

## Environment variables

Common:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_WAREHOUSE_ID`
- `BENCHMARK_REPEATS`

Runner-specific:
- `DATABRICKS_VOLUME_PATH` (`python-volume-download`)
- `DATABRICKS_ODBC_DRIVER` (ODBC runners)
- `JDBC_JAR_PATH` (JDBC runners)
- `SHARING_CATALOG`, `SHARING_SCHEMA`, `SHARING_SHARE`, `SHARING_RECIPIENT`, `SHARING_PROFILE_PATH` (sharing setup/runner overrides)
- `DUCKDB_UC_CATALOG`, `DUCKDB_UC_SCHEMA` (`python-external-duckdb`)
- `DUCKDB_UC_REGION` (optional override; defaults to metastore region via SDK)

## External client setup

Run once before:
- `python-sharing-client`
- `python-sharing-client-hack`
- `python-external-duckdb`

```bash
uv run python scripts/setup_external_clients.py
```

Minimum required env vars for `scripts/setup_external_clients.py`:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_WAREHOUSE_ID`

What the setup script does:
- Creates/replaces `<SHARING_CATALOG>.<SHARING_SCHEMA>.catalog_sales` via `DEEP CLONE`
- Recreates share + recipient and grants `SELECT` on the share
- Ensures metastore external data access is enabled
- Grants `EXTERNAL USE SCHEMA` on the target schema to the current user
- Writes Delta Sharing profile to `SHARING_PROFILE_PATH` (default: `secrets/dbx-fetch-benchmark.share`)

DuckDB runner behavior:
- Attaches `<DUCKDB_UC_CATALOG>` as `uc`
- Rewrites `samples.tpcds_sf1000.catalog_sales` to `uc.<DUCKDB_UC_SCHEMA>.catalog_sales`

## Run benchmarks

Run a single runner:

```bash
uv run python runners/python-sql-connector/run.py
Rscript runners/r-brickster-sql/run.R
```

Run all runners in sequence:

```bash
bash scripts/run_all.sh
```

All runner commands:

```bash
uv run python runners/python-sql-connector/run.py
uv run python runners/python-volume-download/run.py
uv run python runners/python-adbc/run.py
uv run python runners/python-odbc/run.py
uv run python runners/python-jdbc/run.py
uv run python runners/python-sharing-client/run.py
uv run python runners/python-sharing-client-hack/run.py
uv run python runners/python-external-duckdb/run.py
Rscript runners/r-brickster-sql/run.R
Rscript runners/r-adbc/run.R
Rscript runners/r-odbc/run.R
Rscript runners/r-jdbc/run.R
```

Raw benchmark JSON outputs are written to `results/<client-id>/`.

## Aggregate and report

```bash
quarto render report/benchmark_report.qmd
```

Generated outputs:
- `report/benchmark_report.html`
- `results/iterations.csv`
- `results/summary.csv`
