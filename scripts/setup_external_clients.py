#!/usr/bin/env python3
from __future__ import annotations

# libs
import json
import os
from pathlib import Path
from urllib.parse import quote

import databricks.sql as sql
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from common.python.helpers import require_envs, strip_scheme

# variables
source_table = "samples.tpcds_sf1000.catalog_sales"
shared_table_name = "catalog_sales"
default_sharing_catalog = "zacdav"
default_sharing_schema = "dbx_fetch_benchmark_sharing"
default_sharing_share = "dbx_fetch_benchmark_share"
default_sharing_recipient = "dbx_fetch_benchmark_recipient"
default_sharing_profile_path = "secrets/dbx-fetch-benchmark.share"
external_clients = [
    "python-sharing-client",
    "python-sharing-client-hack",
    "python-external-duckdb",
]


# functions
def execute_sql(connection, statement: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(statement)


def create_share_table(connection, catalog: str, schema: str) -> str:
    schema_fqn = f"{catalog}.{schema}"
    shared_table_fqn = f"{schema_fqn}.{shared_table_name}"

    execute_sql(connection, f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")
    print(f"Creating shared table with DEEP CLONE: {shared_table_fqn}")
    execute_sql(connection, f"CREATE OR REPLACE TABLE {shared_table_fqn} DEEP CLONE {source_table}")

    return shared_table_fqn


def recreate_share_and_recipient(
    connection,
    share_name: str,
    recipient_name: str,
    shared_table_fqn: str,
) -> None:
    execute_sql(connection, f"DROP RECIPIENT IF EXISTS {recipient_name}")
    execute_sql(connection, f"DROP SHARE IF EXISTS {share_name}")
    execute_sql(connection, f"CREATE SHARE {share_name}")
    execute_sql(connection, f"ALTER SHARE {share_name} ADD TABLE {shared_table_fqn}")

    execute_sql(connection, f"CREATE RECIPIENT {recipient_name}")
    execute_sql(connection, f"GRANT SELECT ON SHARE {share_name} TO RECIPIENT {recipient_name}")


def ensure_external_data_access_enabled(workspace_client: WorkspaceClient) -> None:
    metastore = workspace_client.metastores.summary()
    if metastore.external_access_enabled:
        print("Metastore external data access already enabled.")
        return
    workspace_client.metastores.update(metastore.metastore_id, external_access_enabled=True)
    print("Enabled metastore external data access.")


def grant_external_use_schema(connection, catalog: str, schema: str) -> None:
    schema_fqn = f"{catalog}.{schema}"
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_user()")
        principal = cursor.fetchone()[0]

    execute_sql(
        connection,
        f"GRANT EXTERNAL USE SCHEMA ON SCHEMA {schema_fqn} TO `{principal}`",
    )
    print(f"Granted EXTERNAL USE SCHEMA on {schema_fqn} to {principal}.")


def write_credential_file(workspace_client: WorkspaceClient, recipient_name: str, profile_path: Path) -> None:
    recipient = workspace_client.recipients.get(recipient_name)
    activation_url = recipient.tokens[0].activation_url
    activation_ref = quote(activation_url, safe="")
    token = workspace_client.recipient_activation.retrieve_token(activation_url=activation_ref)

    payload = {
        "shareCredentialsVersion": token.share_credentials_version or 1,
        "endpoint": token.endpoint,
        "bearerToken": token.bearer_token,
    }
    if token.expiration_time:
        payload["expirationTime"] = token.expiration_time

    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote external access profile: {profile_path}")


# script work
load_dotenv()
creds = require_envs(["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID"])
sharing_catalog = os.getenv("SHARING_CATALOG", default_sharing_catalog)
sharing_schema = os.getenv("SHARING_SCHEMA", default_sharing_schema)
sharing_share = os.getenv("SHARING_SHARE", default_sharing_share)
sharing_recipient = os.getenv("SHARING_RECIPIENT", default_sharing_recipient)
sharing_profile_path = os.getenv("SHARING_PROFILE_PATH", default_sharing_profile_path)

con = sql.connect(
    server_hostname=strip_scheme(creds["DATABRICKS_HOST"]),
    http_path=f"/sql/1.0/warehouses/{creds['DATABRICKS_WAREHOUSE_ID']}",
    access_token=creds["DATABRICKS_TOKEN"],
)
workspace_client = WorkspaceClient(
    host=creds["DATABRICKS_HOST"],
    token=creds["DATABRICKS_TOKEN"],
)

shared_table_fqn = create_share_table(
    con,
    sharing_catalog,
    sharing_schema,
)
recreate_share_and_recipient(
    con,
    sharing_share,
    sharing_recipient,
    shared_table_fqn,
)
ensure_external_data_access_enabled(workspace_client)
grant_external_use_schema(con, sharing_catalog, sharing_schema)
write_credential_file(
    workspace_client,
    sharing_recipient,
    Path(sharing_profile_path),
)
print(
    "External client setup complete for "
    f"{', '.join(external_clients)} "
    f"(share={sharing_share}, schema={sharing_schema}, table={shared_table_name})."
)
