#!/usr/bin/env Rscript

# Required for Arrow access on JDK 16+ with Databricks JDBC v3.
options(java.parameters = c(
  "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
))

# libs
suppressPackageStartupMessages({
  library(DBI)
  library(RJDBC)
  library(fs)
  library(jsonlite)
  library(microbenchmark)
  library(purrr)
})

# variables
path <- fs::path("queries", "scenarios.json")
client_id <- "r-jdbc"
driver_class <- "com.databricks.client.jdbc.Driver"

# functions
source(fs::path("common", "r", "helpers.R"))

# script work
creds <- require_envs(c(
  "DATABRICKS_HOST",
  "DATABRICKS_TOKEN",
  "DATABRICKS_WAREHOUSE_ID",
  "JDBC_JAR_PATH"
))

loaded <- load_queries(path)
repeats <- as.integer(require_env("BENCHMARK_REPEATS"))

jdbc_url <- build_databricks_jdbc_uri(
  creds[["DATABRICKS_HOST"]],
  creds[["DATABRICKS_WAREHOUSE_ID"]]
)

drv <- RJDBC::JDBC(
  driverClass = driver_class,
  classPath = creds[["JDBC_JAR_PATH"]]
)
con <- DBI::dbConnect(drv, jdbc_url, "token", creds[["DATABRICKS_TOKEN"]])

run_query <- function(query) DBI::dbGetQuery(con, query)

results <- run_scenarios(loaded$scenarios, run_query, repeats)

payload <- build_payload(
  schema_version = loaded$queries$schema_version,
  client_id = client_id,
  repeats = repeats,
  results = results
)

write_payload(payload, default_out_path(client_id))
