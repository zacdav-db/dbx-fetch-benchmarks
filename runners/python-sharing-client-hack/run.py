#!/usr/bin/env python3
from __future__ import annotations

# libs
import os
import tempfile
from json import dump, loads
from pathlib import Path
from typing import List, Optional

import delta_kernel_rust_sharing_wrapper
import pyarrow as pa
from delta_sharing.protocol import DeltaSharingProfile, Table
from delta_sharing.rest_client import DataSharingRestClient
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
client_id = "python-sharing-client-hack"
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


def _extract_num_records(delta_single_action: dict) -> Optional[int]:
    add_action = delta_single_action.get("add")
    if not isinstance(add_action, dict):
        return None

    num_records = add_action.get("numRecords")
    if isinstance(num_records, int):
        return num_records

    stats = add_action.get("stats")
    if isinstance(stats, str) and stats:
        try:
            stats_json = loads(stats)
            stats_num_records = stats_json.get("numRecords")
            if isinstance(stats_num_records, int):
                return stats_num_records
        except Exception:
            return None

    return None


def _write_temp_delta_log_snapshot(temp_dir: str, lines: List[str], limit_hint: Optional[int]) -> str:
    table_path = f"file:///{temp_dir}"
    log_dir = os.path.join(temp_dir, "_delta_log")
    os.makedirs(log_dir, exist_ok=True)

    with open(os.path.join(log_dir, "0".zfill(20) + ".json"), "w", encoding="utf-8") as file:
        dump({"protocol": loads(lines[0])["protocol"]["deltaProtocol"]}, file)
        file.write("\n")
        dump({"metaData": loads(lines[1])["metaData"]["deltaMetadata"]}, file)
        file.write("\n")
        remaining_rows = limit_hint
        for line in lines[2:]:
            delta_single_action = loads(line)["file"]["deltaSingleAction"]
            dump(delta_single_action, file)
            file.write("\n")
            if remaining_rows is not None:
                num_records = _extract_num_records(delta_single_action)
                if num_records is not None:
                    remaining_rows -= num_records
                    if remaining_rows <= 0:
                        break

    return table_path


def build_sharing_context(profile_file: str, share: str, schema: str, table: str):
    profile = DeltaSharingProfile.read_from_file(profile_file)
    rest_client = DataSharingRestClient(profile)
    remote_table = Table(name=table, share=share, schema=schema)
    return rest_client, remote_table


def table_to_arrow(rest_client: DataSharingRestClient, remote_table: Table, limit_hint: Optional[int] = None) -> pa.Table:
    rest_client.set_delta_format_header()
    if limit_hint is None:
        response = rest_client.list_files_in_table(remote_table)
    else:
        response = rest_client.list_files_in_table(remote_table, limitHint=limit_hint)

    with tempfile.TemporaryDirectory(prefix="dbx-fetch-benchmark-sharing-hack-") as temp_dir:
        table_uri = _write_temp_delta_log_snapshot(temp_dir, response.lines, limit_hint)
        interface = delta_kernel_rust_sharing_wrapper.PythonInterface(table_uri)
        snapshot = delta_kernel_rust_sharing_wrapper.Table(table_uri).snapshot(interface)
        scan = delta_kernel_rust_sharing_wrapper.ScanBuilder(snapshot).build()
        batch_iterator = iter(scan.execute(interface))

        selected_batches = []
        rows_selected = 0

        while True:
            try:
                batch = next(batch_iterator)
            except StopIteration:
                break
            if limit_hint is None:
                selected_batches.append(batch)
                rows_selected += batch.num_rows
                continue

            remaining = limit_hint - rows_selected
            if remaining <= 0:
                break

            if batch.num_rows <= remaining:
                selected_batches.append(batch)
                rows_selected += batch.num_rows
            else:
                selected_batches.append(batch.slice(0, remaining))
                rows_selected += remaining
                break
        if selected_batches:
            table_result = pa.Table.from_batches(selected_batches)
        else:
            table_result = pa.table({})

    rest_client.remove_delta_format_header()
    return table_result


def run_query_factory(profile_file: str, share: str, schema: str, query_limits: dict[str, int]):
    rest_client, remote_table = build_sharing_context(profile_file, share, schema, shared_table_name)

    def run_query(query: str):
        return table_to_arrow(
            rest_client=rest_client,
            remote_table=remote_table,
            limit_hint=query_limits[query],
        )

    return run_query


# script work
load_dotenv()
creds = require_envs(["BENCHMARK_REPEATS"])
repeats = int(creds["BENCHMARK_REPEATS"])
loaded = load_queries(path)
sharing_profile_path = os.getenv("SHARING_PROFILE_PATH", default_sharing_profile_path)
sharing_share = os.getenv("SHARING_SHARE", default_sharing_share)
sharing_schema = os.getenv("SHARING_SCHEMA", default_sharing_schema)

query_limits = {
    query: extract_limit_from_scenario_id(scenario_id)
    for scenario_id, query in loaded["scenarios"].items()
}

run_query = run_query_factory(sharing_profile_path, sharing_share, sharing_schema, query_limits)
results = run_scenarios(loaded["scenarios"], run_query, repeats)

payload = build_payload(
    schema_version=loaded["queries"]["schema_version"],
    client_id=client_id,
    repeats=repeats,
    results=results,
)

write_payload(payload, default_out_path(client_id))
