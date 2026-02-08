#!/usr/bin/env python3
from __future__ import annotations

# libs
from pathlib import Path

from adbc_driver_manager import dbapi
from dotenv import load_dotenv

from common.python.helpers import (
    build_databricks_adbc_uri,
    build_payload,
    default_out_path,
    load_queries,
    require_envs,
    run_scenarios,
    write_payload,
)

# variables
path = Path("queries") / "scenarios.json"
client_id = "python-adbc"

# functions
def run_query_factory(connection):
    def run_query(query: str):
        with connection.cursor() as cursor:
            cursor.execute(query)
            cursor.fetch_arrow_table()

    return run_query


# script work
load_dotenv()
creds = require_envs(["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID", "BENCHMARK_REPEATS"])
repeats = int(creds["BENCHMARK_REPEATS"])
loaded = load_queries(path)

uri = build_databricks_adbc_uri(
    host=creds["DATABRICKS_HOST"],
    token=creds["DATABRICKS_TOKEN"],
    warehouse_id=creds["DATABRICKS_WAREHOUSE_ID"],
)
con = dbapi.connect(driver="databricks", db_kwargs={"uri": uri})

run_query = run_query_factory(con)
results = run_scenarios(loaded["scenarios"], run_query, repeats)

payload = build_payload(
    schema_version=loaded["queries"]["schema_version"],
    client_id=client_id,
    repeats=repeats,
    results=results,
)

write_payload(payload, default_out_path(client_id))
