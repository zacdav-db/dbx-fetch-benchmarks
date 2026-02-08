require_env <- function(name) {
  value <- Sys.getenv(name, unset = "")
  if (!nzchar(value)) {
    stop(paste0("Missing env var: ", name))
  }
  value
}

require_envs <- function(names) {
  values <- purrr::map_chr(names, require_env)
  stats::setNames(values, names)
}

load_queries <- function(path) {
  if (!fs::file_exists(path)) {
    stop("Missing queries/scenarios.json (run from repo root).")
  }

  queries <- jsonlite::fromJSON(path)
  scenarios <- purrr::set_names(queries$scenarios$sql, queries$scenarios$id)
  list(queries = queries, scenarios = scenarios)
}

default_out_path <- function(client_id) {
  ts <- format(Sys.Date(), "%Y%m%d")
  out_dir <- fs::path("results", client_id)
  fs::dir_create(out_dir, recurse = TRUE)
  fs::path(out_dir, paste("run", ts, sep = "_"), ext = "json")
}

benchmark_seconds <- function(run_query, repeats) {
  run_query()
  mb <- microbenchmark::microbenchmark(run_query(), times = repeats, unit = "s")
  as.numeric(microbenchmark:::convert_to_unit(mb, unit = "s"))
}

run_scenarios <- function(scenarios, run_query, repeats) {
  purrr::imap(scenarios, function(query, scenario_id) {
    message("Running scenario: ", scenario_id)
    run_scenario(
      scenario_id = scenario_id,
      query = query,
      run_query = function() run_query(query),
      repeats = repeats
    )
  })
}

run_scenario <- function(scenario_id, query, run_query, repeats) {
  list(
    scenario = list(id = scenario_id, query = query),
    times = benchmark_seconds(run_query, repeats)
  )
}

build_payload <- function(schema_version, client_id, repeats, results) {
  list(
    schema_version = schema_version,
    client = list(id = client_id, language = "r"),
    parameters = list(repeats = repeats),
    results = results
  )
}

write_payload <- function(payload, out_path) {
  jsonlite::write_json(
    payload,
    out_path,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null"
  )
}

build_databricks_adbc_uri <- function(host, token, warehouse_id) {
  host_clean <- sub("/+$", "", sub("^https?://", "", host))
  http_path <- paste0("sql/1.0/warehouses/", warehouse_id)

  paste0(
    "databricks://token:",
    utils::URLencode(token, reserved = TRUE),
    "@",
    host_clean,
    ":443/",
    http_path
  )
}

build_databricks_jdbc_uri <- function(host, warehouse_id) {
  host_clean <- sub("/+$", "", sub("^https?://", "", host))

  paste0(
    "jdbc:databricks://", host_clean, ":443/default;",
    "transportMode=http;",
    "ssl=1;",
    "httpPath=/sql/1.0/warehouses/", warehouse_id, ";",
    "AuthMech=3;"
  )
}
