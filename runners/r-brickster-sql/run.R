#!/usr/bin/env Rscript

# libs
suppressPackageStartupMessages({
  library(brickster)
  library(fs)
  library(jsonlite)
  library(microbenchmark)
  library(purrr)
})

# variables
path <- fs::path("queries", "scenarios.json")
client_id <- "r-brickster-sql"

# functions
source(fs::path("common", "r", "helpers.R"))

# check we have what we need
warehouse_id <- require_env("DATABRICKS_WAREHOUSE_ID")
repeats <- as.integer(require_env("BENCHMARK_REPEATS"))

loaded <- load_queries(path)

run_query <- function(query) {
  brickster::db_sql_query(
    warehouse_id = warehouse_id,
    statement = query,
    return_arrow = TRUE,
    disposition = "EXTERNAL_LINKS",
    show_progress = FALSE
  )
}

results <- run_scenarios(loaded$scenarios, run_query, repeats)

payload <- build_payload(
  schema_version = loaded$queries$schema_version,
  client_id = client_id,
  repeats = repeats,
  results = results
)

write_payload(payload, default_out_path(client_id))
