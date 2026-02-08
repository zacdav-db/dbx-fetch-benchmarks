#!/usr/bin/env python3
from __future__ import annotations

# libs
import os
from pathlib import Path

import duckdb
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from common.python.helpers import (
    build_payload,
    default_out_path,
    load_queries,
    require_envs,
    run_scenarios,
    write_payload,
)

# variables
path = Path("queries") / "scenarios.json"
client_id = "python-external-duckdb"
source_table = "samples.tpcds_sf1000.catalog_sales"
shared_table_name = "catalog_sales"
uc_name = "uc"


# functions
def attach_unity_catalog(
    con: duckdb.DuckDBPyConnection,
    catalog: str,
    name: str,
    token: str,
    endpoint: str,
    region: str,
) -> None:
    con.execute("INSTALL unity_catalog")
    con.execute("INSTALL delta FROM core")
    con.execute("LOAD delta")
    con.execute("LOAD unity_catalog")
    con.execute(
        f"""
        CREATE SECRET (
          TYPE UC,
          TOKEN '{token}',
          ENDPOINT '{endpoint}',
          AWS_REGION '{region}'
        )
        """
    )
    con.execute(f"ATTACH '{catalog}' AS \"{name}\" (TYPE UC_CATALOG)")


def run_query_factory(con: duckdb.DuckDBPyConnection, target_table_fqn: str):
    def run_query(query: str):
        rewritten_query = query.replace(source_table, target_table_fqn)
        return con.execute(rewritten_query).fetch_arrow_table()

    return run_query


# script work
load_dotenv()
creds = require_envs(
    [
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "BENCHMARK_REPEATS",
        "DUCKDB_UC_CATALOG",
        "DUCKDB_UC_SCHEMA",
    ]
)
repeats = int(creds["BENCHMARK_REPEATS"])
loaded = load_queries(path)
workspace_client = WorkspaceClient(
    host=creds["DATABRICKS_HOST"],
    token=creds["DATABRICKS_TOKEN"],
)
region = os.getenv("DUCKDB_UC_REGION", workspace_client.metastores.summary().region or "")
if not region:
    raise RuntimeError("Missing metastore region. Set DUCKDB_UC_REGION.")

target_table_fqn = f"{uc_name}.{creds['DUCKDB_UC_SCHEMA']}.{shared_table_name}"

con = duckdb.connect()
attach_unity_catalog(
    con=con,
    catalog=creds["DUCKDB_UC_CATALOG"],
    name=uc_name,
    token=creds["DATABRICKS_TOKEN"],
    endpoint=creds["DATABRICKS_HOST"],
    region=region,
)

run_query = run_query_factory(con, target_table_fqn)
results = run_scenarios(loaded["scenarios"], run_query, repeats)

payload = build_payload(
    schema_version=loaded["queries"]["schema_version"],
    client_id=client_id,
    repeats=repeats,
    results=results,
)

write_payload(payload, default_out_path(client_id))
