#!/usr/bin/env python3
from __future__ import annotations

# libs
import os
from pathlib import Path

import delta_sharing
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
client_id = "python-sharing-client"
shared_table_name = "catalog_sales"
default_sharing_schema = "dbx_fetch_benchmark_sharing"
default_sharing_share = "dbx_fetch_benchmark_share"
default_sharing_profile_path = "secrets/dbx-fetch-benchmark.share"


# functions
def extract_limit_from_scenario_id(scenario_id: str) -> int:
    limit_token = scenario_id.rsplit("_", 1)[-1]
    if limit_token.isdigit():
        return int(limit_token)
    if scenario_id.endswith("single_row"):
        return 1
    raise RuntimeError(f"Could not parse limit from scenario id: {scenario_id}")


def build_table_url(profile_path: str, share_name: str, schema_name: str, table_name: str) -> str:
    return f"{profile_path}#{share_name}.{schema_name}.{table_name}"


def run_query_factory(table_url: str, query_limits: dict[str, int]):
    def run_query(query: str):
        return delta_sharing.load_as_pandas(table_url, limit=query_limits[query])

    return run_query


# script work
load_dotenv()
creds = require_envs(["BENCHMARK_REPEATS"])
repeats = int(creds["BENCHMARK_REPEATS"])
loaded = load_queries(path)
sharing_profile_path = os.getenv("SHARING_PROFILE_PATH", default_sharing_profile_path)
sharing_share = os.getenv("SHARING_SHARE", default_sharing_share)
sharing_schema = os.getenv("SHARING_SCHEMA", default_sharing_schema)

table_url = build_table_url(
    sharing_profile_path,
    sharing_share,
    sharing_schema,
    shared_table_name,
)
query_limits = {
    query: extract_limit_from_scenario_id(scenario_id)
    for scenario_id, query in loaded["scenarios"].items()
}

run_query = run_query_factory(table_url, query_limits)
results = run_scenarios(loaded["scenarios"], run_query, repeats)

payload = build_payload(
    schema_version=loaded["queries"]["schema_version"],
    client_id=client_id,
    repeats=repeats,
    results=results,
)

write_payload(payload, default_out_path(client_id))
