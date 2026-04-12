install.packages(c("CDMConnector", "duckdb", "DBI"))
library(CDMConnector)

find_script_dir <- function() {
  frame_ids <- rev(seq_along(sys.frames()))
  for (i in frame_ids) {
    ofile <- sys.frames()[[i]]$ofile
    if (!is.null(ofile)) {
      return(dirname(normalizePath(ofile, winslash = "/", mustWork = FALSE)))
    }
  }

  file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  if (length(file_arg) > 0) {
    script_path <- sub("^--file=", "", file_arg[1])
    return(dirname(normalizePath(script_path, winslash = "/", mustWork = FALSE)))
  }

  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

find_project_root <- function() {
  script_dir <- find_script_dir()
  if (basename(script_dir) == "r") {
    dirname(script_dir)
  } else {
    script_dir
  }
}

load_project_renviron <- function(project_root) {
  candidates <- unique(c(
    file.path(project_root, ".Renviron"),
    file.path(getwd(), ".Renviron")
  ))

  for (candidate in candidates) {
    if (file.exists(candidate)) {
      readRenviron(candidate)
      return(invisible(candidate))
    }
  }

  invisible(NULL)
}

resolve_project_path <- function(path, project_root) {
  if (grepl("^(?:[A-Za-z]:[/\\\\]|/)", path)) {
    path
  } else {
    file.path(project_root, path)
  }
}

project_root <- find_project_root()
load_project_renviron(project_root)

get_env_required <- function(name) {
  value <- Sys.getenv(name, unset = "")
  if (identical(value, "")) {
    stop(sprintf("Environment variable %s must be set.", name), call. = FALSE)
  }
  value
}

get_env_or <- function(name, default) {
  value <- Sys.getenv(name, unset = "")
  if (identical(value, "")) default else value
}

Sys.setenv(EUNOMIA_DATA_FOLDER = get_env_required("PLP_EUNOMIA_CACHE_DIR"))

project_data_dir <- resolve_project_path(
  get_env_or("PLP_PROJECT_DATA_DIR", "data"),
  project_root
)
if (!dir.exists(project_data_dir)) {
  dir.create(project_data_dir, recursive = TRUE)
}

duckdb_file <- resolve_project_path(
  get_env_or(
    "PLP_EUNOMIA_DUCKDB_PATH",
    file.path(project_data_dir, "eunomia_synthea-allergies-10k.duckdb")
  ),
  project_root
)

eunomia_path <- eunomiaDir(
  datasetName = "synthea-allergies-10k",
  cdmVersion = "5.3",
  databaseFile = duckdb_file
)

cat("DuckDB file:", eunomia_path, "\n")
cat("Exists:", file.exists(eunomia_path), "\n")
cat("Size:", file.info(eunomia_path)$size, "bytes\n")

