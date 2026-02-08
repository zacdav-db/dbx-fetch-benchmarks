from __future__ import annotations

import json
import os
import time
from datetime import date
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote


def require_env(name: str) -> str:
    value = os.getenv(name, "")
    if not value:
        raise RuntimeError(f"Missing env var: {name}")
    return value


def require_envs(names: list[str]) -> dict[str, str]:
    return {name: require_env(name) for name in names}


def load_queries(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError("Missing queries/scenarios.json (run from repo root).")

    queries = json.loads(path.read_text(encoding="utf-8"))
    scenarios = {scenario["id"]: scenario["sql"] for scenario in queries["scenarios"]}
    return {"queries": queries, "scenarios": scenarios}


def default_out_path(client_id: str) -> Path:
    out_dir = Path("results") / client_id
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"run_{date.today().strftime('%Y%m%d')}.json"


def benchmark_seconds(run_query: Callable[[], Any], repeats: int) -> list[float]:
    run_query()
    times: list[float] = []
    for _ in range(repeats):
        start = time.perf_counter()
        run_query()
        times.append(time.perf_counter() - start)
    return times


def run_scenario(
    scenario_id: str,
    query: str,
    run_query: Callable[[], Any],
    repeats: int,
) -> dict[str, Any]:
    return {
        "scenario": {"id": scenario_id, "query": query},
        "times": benchmark_seconds(run_query, repeats),
    }


def run_scenarios(
    scenarios: dict[str, str],
    run_query: Callable[[str], Any],
    repeats: int,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for scenario_id, query in scenarios.items():
        print(f"Running scenario: {scenario_id}")
        results.append(
            run_scenario(
                scenario_id=scenario_id,
                query=query,
                run_query=lambda query=query: run_query(query),
                repeats=repeats
            )
        )
    return results


def build_payload(
    schema_version: int,
    client_id: str,
    repeats: int,
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "client": {"id": client_id, "language": "python"},
        "parameters": {"repeats": repeats},
        "results": results,
    }


def fetch_query_result(cursor: Any) -> Any:
    if hasattr(cursor, "fetchall_arrow"):
        return cursor.fetchall_arrow()
    if hasattr(cursor, "fetchallarrow"):
        return cursor.fetchallarrow()
    if hasattr(cursor, "fetch_arrow_table"):
        return cursor.fetch_arrow_table()

    rows = cursor.fetchall()
    if rows and getattr(cursor, "description", None):
        columns = [column[0] for column in cursor.description]
        try:
            import pandas as pd
        except Exception:
            return rows
        return pd.DataFrame.from_records(rows, columns=columns)
    return rows


def write_payload(payload: dict[str, Any], out_path: Path) -> None:
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def strip_scheme(host: str) -> str:
    cleaned = host.strip()
    cleaned = cleaned.removeprefix("https://").removeprefix("http://")
    return cleaned.rstrip("/")


def build_databricks_jdbc_uri(host: str, warehouse_id: str) -> str:
    return (
        f"jdbc:databricks://{strip_scheme(host)}:443/default;"
        "transportMode=http;"
        "ssl=1;"
        f"httpPath=/sql/1.0/warehouses/{warehouse_id};"
        "AuthMech=3;"
    )


def build_databricks_adbc_uri(host: str, token: str, warehouse_id: str) -> str:
    encoded_token = quote(token, safe="")
    return (
        f"databricks://token:{encoded_token}@{strip_scheme(host)}:443/"
        f"sql/1.0/warehouses/{warehouse_id}"
    )


def build_databricks_odbc_connection_string(
    host: str,
    token: str,
    warehouse_id: str,
    driver: str,
) -> str:
    return (
        f"Driver={{{driver}}};"
        f"Host={strip_scheme(host)};"
        "Port=443;"
        f"HTTPPath=/sql/1.0/warehouses/{warehouse_id};"
        "AuthMech=3;"
        "UID=token;"
        f"PWD={token};"
        "SSL=1;"
        "ThriftTransport=2;"
    )
