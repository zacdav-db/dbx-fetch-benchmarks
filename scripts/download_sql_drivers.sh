#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRIVERS_DIR="${DRIVERS_DIR:-$REPO_ROOT/drivers}"
ODBC_DOWNLOAD_URL="${DATABRICKS_ODBC_DOWNLOAD_URL:-}"
ODBC_DOCS_URL="https://docs.databricks.com/en/integrations/odbc/download.html"

mkdir -p "$DRIVERS_DIR"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

download_latest_jdbc() {
  require_cmd curl
  require_cmd jq

  local release_json
  local asset_url
  local asset_name
  local out_path

  release_json="$(curl -fsSL https://api.github.com/repos/databricks/databricks-jdbc/releases/latest)"
  asset_url="$(printf '%s' "$release_json" | jq -r '.assets[] | select(.name | test("databricks-jdbc-.*\\.jar$")) | .browser_download_url' | head -n 1)"
  if [[ -z "$asset_url" || "$asset_url" == "null" ]]; then
    echo "Could not find JDBC jar asset in latest release." >&2
    exit 1
  fi

  asset_name="$(basename "$asset_url")"
  out_path="$DRIVERS_DIR/$asset_name"
  echo "Downloading JDBC: $asset_name"
  curl -fL "$asset_url" -o "$out_path"
  printf '%s' "$out_path"
}

download_odbc_package() {
  require_cmd curl
  if [[ -z "$ODBC_DOWNLOAD_URL" ]]; then
    return 1
  fi

  local filename
  local out_path
  filename="$(basename "$ODBC_DOWNLOAD_URL")"
  out_path="$DRIVERS_DIR/$filename"

  echo "Downloading ODBC package: $filename"
  curl -fL "$ODBC_DOWNLOAD_URL" -o "$out_path"
  printf '%s' "$out_path"
}

extract_odbc_zip() {
  local archive_path="$1"
  local extract_dir="$DRIVERS_DIR/odbc"
  require_cmd unzip
  rm -rf "$extract_dir"
  mkdir -p "$extract_dir"
  unzip -q "$archive_path" -d "$extract_dir"
}

detect_odbc_driver_path() {
  local path=""
  local candidate
  local candidates=(
    "/Library/simba/spark/lib/libsparkodbc_sb64-universal.dylib"
    "/Library/simba/spark/lib/libsparkodbc_sb64.dylib"
    "/opt/simba/spark/lib/64/libsparkodbc_sb64.so"
    "/opt/simba/spark/lib/64/libsparkodbc_sbu.so"
  )

  for candidate in "${candidates[@]}"; do
    if [[ -f "$candidate" ]]; then
      path="$candidate"
      break
    fi
  done

  if [[ -z "$path" ]]; then
    path="$(find "$DRIVERS_DIR" -type f \( -name 'libsparkodbc*.dylib' -o -name 'libsparkodbc*.so' \) | head -n 1 || true)"
  fi

  printf '%s' "$path"
}

jdbc_path="$(download_latest_jdbc)"
odbc_package=""
if odbc_package="$(download_odbc_package 2>/dev/null)"; then
  if [[ "$odbc_package" == *.zip ]]; then
    extract_odbc_zip "$odbc_package"
  fi
fi
odbc_driver_path="$(detect_odbc_driver_path)"

echo
echo "Set these env vars:"
echo "JDBC_JAR_PATH=$jdbc_path"
if [[ -n "$odbc_driver_path" ]]; then
  echo "DATABRICKS_ODBC_DRIVER=$odbc_driver_path"
else
  echo "DATABRICKS_ODBC_DRIVER=<path-to-odbc-driver-library>"
  echo "Could not auto-detect an installed ODBC library."
  echo "Install/download the Databricks ODBC driver, then re-run this script:"
  echo "  $ODBC_DOCS_URL"
  echo
  echo "You can also set DATABRICKS_ODBC_DOWNLOAD_URL before running this script."
fi

