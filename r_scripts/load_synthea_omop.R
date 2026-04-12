devtools::install_github("OHDSI/ETL-Synthea")

library(ETLSyntheaBuilder)

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

# We are loading a version 5.4 CDM into a local PostgreSQL database called "synthea10".
# The ETLSyntheaBuilder package leverages the OHDSI/CommonDataModel package for CDM creation.
# Valid CDM versions are determined by executing CommonDataModel::listSupportedVersions().
# The strings representing supported CDM versions are currently "5.3" and "5.4". 
# The Synthea version we use in this example is 2.7.0.
# However, at this time we also support 3.0.0, 3.1.0, 3.2.0 and 3.3.0.
# Please note that Synthea's MASTER branch is always active and this package will be updated to support
# future versions as possible.
# The schema to load the Synthea tables is called "native".
# The schema to load the Vocabulary and CDM tables is "cdm_synthea10".  
# The username and pw are "postgres" and "lollipop".
# The Synthea and Vocabulary CSV files are located in /tmp/synthea/output/csv and /tmp/Vocabulary_20181119, respectively.

# For those interested in seeing the CDM changes from 5.3 to 5.4, please see: http://ohdsi.github.io/CommonDataModel/cdm54Changes.html
DatabaseConnector::downloadJdbcDrivers(
  dbms = "postgresql",
  pathToDriver = get_env_required("PLP_JDBC_PATH")
)

cd <- DatabaseConnector::createConnectionDetails(
  dbms     = "postgresql", 
  server   = get_env_or("PLP_DB_SERVER", "localhost/syntheaxl"), 
  user     = get_env_required("PLP_DB_USER"), 
  password = get_env_required("PLP_DB_PASSWORD"), 
  port     = as.integer(get_env_or("PLP_DB_PORT", "5432")), 
  pathToDriver = get_env_required("PLP_JDBC_PATH")
)

cdmSchema      <- get_env_or("PLP_CDM_SCHEMA", "syntheaxl")
cdmVersion     <- get_env_or("PLP_CDM_VERSION", "5.4")
syntheaVersion <- get_env_or("PLP_SYNTHEA_VERSION", "3.3.0")
syntheaSchema  <- get_env_or("PLP_SYNTHEA_SCHEMA", "native")
syntheaFileLoc <- get_env_required("PLP_SYNTHEA_FILE_LOC")
vocabFileLoc   <- get_env_required("PLP_VOCAB_FILE_LOC")

Sys.setenv(POSTGRES_PATH = get_env_required("PLP_POSTGRES_PATH"))

file.exists(file.path(Sys.getenv("POSTGRES_PATH"), "psql.exe"))

conn <- DatabaseConnector::connect(cd)

DatabaseConnector::executeSql(conn, "CREATE SCHEMA IF NOT EXISTS native;")
DatabaseConnector::executeSql(conn, "CREATE SCHEMA IF NOT EXISTS syntheaxl;")

DatabaseConnector::disconnect(conn)

ETLSyntheaBuilder::CreateCDMTables(connectionDetails = cd, cdmSchema = cdmSchema, cdmVersion = cdmVersion)

ETLSyntheaBuilder::CreateSyntheaTables(connectionDetails = cd, syntheaSchema = syntheaSchema, syntheaVersion = syntheaVersion)

ETLSyntheaBuilder::LoadSyntheaTables(connectionDetails = cd, syntheaSchema = syntheaSchema, syntheaFileLoc = syntheaFileLoc, bulkLoad = TRUE)

ETLSyntheaBuilder::LoadVocabFromCsv(connectionDetails = cd, cdmSchema = cdmSchema, vocabFileLoc = vocabFileLoc, bulkLoad = TRUE)

ETLSyntheaBuilder::CreateMapAndRollupTables(connectionDetails = cd, cdmSchema = cdmSchema, syntheaSchema = syntheaSchema, cdmVersion = cdmVersion, syntheaVersion = syntheaVersion)

## Optional Step to create extra indices
ETLSyntheaBuilder::CreateExtraIndices(connectionDetails = cd, cdmSchema = cdmSchema, syntheaSchema = syntheaSchema, syntheaVersion = syntheaVersion)

ETLSyntheaBuilder::LoadEventTables(connectionDetails = cd, cdmSchema = cdmSchema, syntheaSchema = syntheaSchema, cdmVersion = cdmVersion, syntheaVersion = syntheaVersion) ok keep this script and add the changes

