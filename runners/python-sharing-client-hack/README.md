# python-sharing-client-hack

## Summary
Benchmarks fetch through Delta Sharing plus `delta_kernel_rust_sharing_wrapper`, returning in-memory Arrow tables.

## Setup
1. Install Python dependencies:

```bash
uv sync
```

2. Prepare share, recipient, and profile:

```bash
uv run python scripts/setup_external_clients.py
```

## Environment
Required:
- `BENCHMARK_REPEATS`

Optional (defaults shown):
- `SHARING_PROFILE_PATH` (`secrets/dbx-fetch-benchmark.share`)
- `SHARING_SHARE` (`dbx_fetch_benchmark_share`)
- `SHARING_SCHEMA` (`dbx_fetch_benchmark_sharing`)

Behavior:
- Query limits are derived from scenario IDs (for example `narrow_10000` -> `10000`).

## Run
Run from repo root:

```bash
uv run python runners/python-sharing-client-hack/run.py
```
