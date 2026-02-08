#!/usr/bin/env python3
from __future__ import annotations

# libs
from pathlib import Path

import databricks.sql as sql
from dotenv import load_dotenv

from common.python.helpers import (
    build_payload,
    default_out_path,
    load_queries,
    require_envs,
    run_scenarios,
    strip_scheme,
    write_payload,
)

# variables
path = Path("queries") / "scenarios.json"
client_id = "python-sql-connector"

# functions
def run_query_factory(connection):
    def run_query(query: str):
        with connection.cursor() as cursor:
            cursor.execute(query)
            cursor.fetchall_arrow()

    return run_query


# script work
load_dotenv()
creds = require_envs(["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID", "BENCHMARK_REPEATS"])
repeats = int(creds["BENCHMARK_REPEATS"])
loaded = load_queries(path)

con = sql.connect(
    server_hostname=strip_scheme(creds["DATABRICKS_HOST"]),
    http_path=f"/sql/1.0/warehouses/{creds['DATABRICKS_WAREHOUSE_ID']}",
    access_token=creds["DATABRICKS_TOKEN"],
)

run_query = run_query_factory(con)
results = run_scenarios(loaded["scenarios"], run_query, repeats)

payload = build_payload(
    schema_version=loaded["queries"]["schema_version"],
    client_id=client_id,
    repeats=repeats,
    results=results,
)

write_payload(payload, default_out_path(client_id))
