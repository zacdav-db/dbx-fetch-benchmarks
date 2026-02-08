#!/usr/bin/env python3
from __future__ import annotations

# libs
from pathlib import Path

import pyodbc
from dotenv import load_dotenv

from common.python.helpers import (
    build_databricks_odbc_connection_string,
    build_payload,
    default_out_path,
    fetch_query_result,
    load_queries,
    require_envs,
    run_scenarios,
    write_payload,
)

# variables
path = Path("queries") / "scenarios.json"
client_id = "python-odbc"

# functions
def run_query_factory(connection: pyodbc.Connection):
    def run_query(query: str):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            fetch_query_result(cursor)
        finally:
            cursor.close()

    return run_query


# script work
load_dotenv()
creds = require_envs(
    [
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_WAREHOUSE_ID",
        "DATABRICKS_ODBC_DRIVER",
        "BENCHMARK_REPEATS",
    ]
)
repeats = int(creds["BENCHMARK_REPEATS"])
loaded = load_queries(path)

conn_str = build_databricks_odbc_connection_string(
    host=creds["DATABRICKS_HOST"],
    token=creds["DATABRICKS_TOKEN"],
    warehouse_id=creds["DATABRICKS_WAREHOUSE_ID"],
    driver=creds["DATABRICKS_ODBC_DRIVER"],
)
con = pyodbc.connect(conn_str, autocommit=True)

run_query = run_query_factory(con)
results = run_scenarios(loaded["scenarios"], run_query, repeats)

payload = build_payload(
    schema_version=loaded["queries"]["schema_version"],
    client_id=client_id,
    repeats=repeats,
    results=results,
)

write_payload(payload, default_out_path(client_id))
