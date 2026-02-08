#!/usr/bin/env Rscript

# libs
suppressPackageStartupMessages({
  library(DBI)
  library(odbc)
  library(fs)
  library(jsonlite)
  library(microbenchmark)
  library(purrr)
})

# variables
path <- fs::path("queries", "scenarios.json")
client_id <- "r-odbc"

# functions
source(fs::path("common", "r", "helpers.R"))

# script work
warehouse_id <- require_env("DATABRICKS_WAREHOUSE_ID")
host <- require_env("DATABRICKS_HOST")
repeats <- as.integer(require_env("BENCHMARK_REPEATS"))

loaded <- load_queries(path)

con <- DBI::dbConnect(
  odbc::databricks(),
  httpPath = paste0("/sql/1.0/warehouses/", warehouse_id),
  workspace = host
)

run_query <- function(query) DBI::dbGetQuery(con, query)

results <- run_scenarios(loaded$scenarios, run_query, repeats)

payload <- build_payload(
  schema_version = loaded$queries$schema_version,
  client_id = client_id,
  repeats = repeats,
  results = results
)

write_payload(payload, default_out_path(client_id))
