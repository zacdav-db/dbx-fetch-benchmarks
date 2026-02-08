#!/usr/bin/env python3
from __future__ import annotations

# libs
import tempfile
import time
from pathlib import Path

import databricks.sql as sql
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
import pyarrow.dataset as ds

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
client_id = "python-volume-download"
files_per_export = 1
download_parallelism = 10


# functions
def build_export_sql(query: str, remote_dir: str) -> str:
    query_text = query.strip().rstrip(";")
    return (
        f"INSERT OVERWRITE DIRECTORY '{remote_dir}' "
        "USING PARQUET "
        "OPTIONS ('compression' = 'zstd') "
        f"SELECT /*+ REPARTITION({files_per_export}) */ * "
        f"FROM ({query_text}) AS benchmark_query"
    )

def clean_remote_dir(files_client, remote_dir: str) -> None:
    entries = list(files_client.list_directory_contents(remote_dir))
    for entry in entries:
        files_client.delete(str(entry.path))
    files_client.delete_directory(remote_dir)


def read_local_arrow_table(local_dir: Path):
    dataset = ds.dataset(str(local_dir), format="parquet")
    return dataset.to_table()


def prepare_remote_exports(connection, files_client, scenarios: dict[str, str], volume_path: str):
    run_id = str(time.time_ns())
    run_root = f"{volume_path.rstrip('/')}/dbx-fetch-benchmark/{run_id}"
    scenario_dirs: dict[str, str] = {}
    query_files: dict[str, list[str]] = {}

    for scenario_id, query in scenarios.items():
        remote_dir = f"{run_root}/{scenario_id}"
        print(f"Materializing scenario: {scenario_id}")
        export_sql = build_export_sql(query, remote_dir)
        with connection.cursor() as cursor:
            cursor.execute(export_sql)
        scenario_dirs[scenario_id] = remote_dir
        entries = list(files_client.list_directory_contents(remote_dir))
        query_files[query] = [
            str(entry.path) for entry in entries if str(entry.path).endswith(".parquet")
        ]

    return run_root, scenario_dirs, query_files

def run_query_factory(files_client, query_files: dict[str, list[str]]):
    def run_query(query: str):
        remote_files = query_files[query]
        with tempfile.TemporaryDirectory(prefix="dbx-fetch-benchmark-") as temp_dir:
            local_dir = Path(temp_dir)
            for remote_path in remote_files:
                local_path = local_dir / Path(remote_path).name
                files_client.download_to(
                    remote_path,
                    str(local_path),
                    overwrite=True,
                    use_parallel=True,
                    parallelism=download_parallelism,
                )
            return read_local_arrow_table(local_dir)

    return run_query


# script work
load_dotenv()
creds = require_envs(
    [
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_WAREHOUSE_ID",
        "DATABRICKS_VOLUME_PATH",
        "BENCHMARK_REPEATS",
    ]
)
repeats = int(creds["BENCHMARK_REPEATS"])
loaded = load_queries(path)

con = sql.connect(
    server_hostname=strip_scheme(creds["DATABRICKS_HOST"]),
    http_path=f"/sql/1.0/warehouses/{creds['DATABRICKS_WAREHOUSE_ID']}",
    access_token=creds["DATABRICKS_TOKEN"],
)
workspace_client = WorkspaceClient(
    host=creds["DATABRICKS_HOST"],
    token=creds["DATABRICKS_TOKEN"],
)
files_client = workspace_client.files

run_root = ""
scenario_dirs: dict[str, str] = {}
results = []
try:
    run_root, scenario_dirs, query_files = prepare_remote_exports(
        con, files_client, loaded["scenarios"], creds["DATABRICKS_VOLUME_PATH"]
    )
    run_query = run_query_factory(files_client, query_files)
    results = run_scenarios(loaded["scenarios"], run_query, repeats)
finally:
    for remote_dir in scenario_dirs.values():
        clean_remote_dir(files_client, remote_dir)
    if run_root:
        files_client.delete_directory(run_root)

payload = build_payload(
    schema_version=loaded["queries"]["schema_version"],
    client_id=client_id,
    repeats=repeats,
    results=results,
)

write_payload(payload, default_out_path(client_id))
