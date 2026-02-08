#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Syncing Python dependencies (uv)"
uv sync

echo "Running python-sql-connector"
uv run python runners/python-sql-connector/run.py

echo "Running python-volume-download"
uv run python runners/python-volume-download/run.py

echo "Running python-adbc"
uv run python runners/python-adbc/run.py

echo "Running python-odbc"
uv run python runners/python-odbc/run.py

echo "Running python-jdbc"
uv run python runners/python-jdbc/run.py

echo "Running python-sharing-client"
uv run python runners/python-sharing-client/run.py

echo "Running python-sharing-client-hack"
uv run python runners/python-sharing-client-hack/run.py

echo "Running python-external-duckdb"
uv run python runners/python-external-duckdb/run.py

echo "Running r-brickster-sql"
Rscript runners/r-brickster-sql/run.R

echo "Running r-adbc"
Rscript runners/r-adbc/run.R

echo "Running r-odbc"
Rscript runners/r-odbc/run.R

echo "Running r-jdbc"
Rscript runners/r-jdbc/run.R
