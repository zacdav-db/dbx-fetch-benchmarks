#!/usr/bin/env python3
from __future__ import annotations

# libs
from pathlib import Path

import jaydebeapi
import jpype
from dotenv import load_dotenv

from common.python.helpers import (
    build_databricks_jdbc_uri,
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
client_id = "python-jdbc"
driver_class = "com.databricks.client.jdbc.Driver"
arrow_add_opens = "--add-opens=java.base/java.nio=ALL-UNNAMED"

# functions
def run_query_factory(connection):
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
    ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID", "JDBC_JAR_PATH", "BENCHMARK_REPEATS"]
)
repeats = int(creds["BENCHMARK_REPEATS"])
loaded = load_queries(path)

if not jpype.isJVMStarted():
    jpype.startJVM(
        jpype.getDefaultJVMPath(),
        arrow_add_opens,
        f"-Djava.class.path={creds['JDBC_JAR_PATH']}",
    )

jdbc_url = build_databricks_jdbc_uri(creds["DATABRICKS_HOST"], creds["DATABRICKS_WAREHOUSE_ID"])
con = jaydebeapi.connect(
    driver_class,
    jdbc_url,
    ["token", creds["DATABRICKS_TOKEN"]],
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
