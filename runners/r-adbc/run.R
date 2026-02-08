#!/usr/bin/env Rscript

# libs
suppressPackageStartupMessages({
  library(adbcdrivermanager)
  library(fs)
  library(jsonlite)
  library(microbenchmark)
  library(nanoarrow)
  library(purrr)
})

# variables
path <- fs::path("queries", "scenarios.json")
client_id <- "r-adbc"

# functions
source(fs::path("common", "r", "helpers.R"))

# script work
creds <- require_envs(c("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID"))
host <- creds[["DATABRICKS_HOST"]]
token <- creds[["DATABRICKS_TOKEN"]]
warehouse_id <- creds[["DATABRICKS_WAREHOUSE_ID"]]
repeats <- as.integer(require_env("BENCHMARK_REPEATS"))

loaded <- load_queries(path)
uri <- build_databricks_adbc_uri(host, token, warehouse_id)

driver <- adbcdrivermanager::adbc_driver("databricks")
db <- adbcdrivermanager::adbc_database_init(driver, uri = uri)
con <- adbcdrivermanager::adbc_connection_init(db)

run_query <- function(query) {
  arrow::as_arrow_table(adbcdrivermanager::read_adbc(con, query))
}

results <- run_scenarios(loaded$scenarios, run_query, repeats)

payload <- build_payload(
  schema_version = loaded$queries$schema_version,
  client_id = client_id,
  repeats = repeats,
  results = results
)

write_payload(payload, default_out_path(client_id))
