# Simulate the current Python P-PLP setup in R / RStudio using OHDSI packages.
# Current Python defaults mirrored here:
# - PostgreSQL database: synthealarge on localhost:5432
# - CDM schema: synthealarge
# - Work schema: plp_work
# - Target concept: 40481087
# - Outcome concept: 40481087
# - Outpatient visit concept: 9202
# - Time-at-risk: days 1-365 after index
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

required_packages <- c(
  "remotes",
  "DatabaseConnector",
  "SqlRender",
  "FeatureExtraction",
  "PatientLevelPrediction"
)

install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
}

invisible(lapply(required_packages, install_if_missing))

install_github_if_missing <- function(pkg, repo) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    remotes::install_github(repo, upgrade = "never")
  }
}

install_github_if_missing("DatabaseConnector", "OHDSI/DatabaseConnector")
install_github_if_missing("SqlRender", "OHDSI/SqlRender")
install_github_if_missing("FeatureExtraction", "OHDSI/FeatureExtraction")
install_github_if_missing("PatientLevelPrediction", "OHDSI/PatientLevelPrediction")

library(DatabaseConnector)
library(SqlRender)
library(FeatureExtraction)
library(PatientLevelPrediction)

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

# JDBC driver location for DatabaseConnector
path_to_driver <- get_env_required("PLP_JDBC_PATH")
if (!dir.exists(path_to_driver)) {
  dir.create(path_to_driver, recursive = TRUE)
}

driver_files <- list.files(path_to_driver, pattern = "\\.jar$", full.names = TRUE)
if (length(driver_files) == 0) {
  DatabaseConnector::downloadJdbcDrivers(
    dbms = "postgresql",
    pathToDriver = path_to_driver
  )
}

# Database settings matching the current Python .env/config
dbms <- "postgresql"
server <- get_env_or("PLP_DB_SERVER", "localhost/synthealarge")
port <- as.integer(get_env_or("PLP_DB_PORT", "5432"))
user <- get_env_required("PLP_DB_USER")
password <- get_env_required("PLP_DB_PASSWORD")

cdm_database_schema <- get_env_or("PLP_CDM_SCHEMA", "synthealarge")
cohort_database_schema <- get_env_or("PLP_WORK_SCHEMA", "plp_work")
cohort_table <- get_env_or("PLP_COHORT_TABLE", "plp_cohort")
oracle_temp_schema <- NULL

# Cohort logic matching the current Python pipeline defaults
target_cohort_id <- 1
outcome_cohort_id <- 2
target_concept_id <- 40481087
outcome_concept_id <- 40481087
outpatient_visit_concept_id <- 9202
risk_window_start <- 1
risk_window_end <- 365
lookback_days <- 365

connection_details <- DatabaseConnector::createConnectionDetails(
  dbms = dbms,
  server = server,
  port = port,
  user = user,
  password = password,
  pathToDriver = path_to_driver
)

conn <- DatabaseConnector::connect(connection_details)
on.exit(DatabaseConnector::disconnect(conn), add = TRUE)

sql <- "
CREATE SCHEMA IF NOT EXISTS @cohort_database_schema;

DROP TABLE IF EXISTS @cohort_database_schema.target_cohort;
CREATE TABLE @cohort_database_schema.target_cohort AS
WITH qualifying_visits AS (
  SELECT
    co.person_id AS subject_id,
    co.condition_start_date AS cohort_start_date,
    ROW_NUMBER() OVER (
      PARTITION BY co.person_id
      ORDER BY co.condition_start_date, co.condition_occurrence_id
    ) AS rn
  FROM @cdm_database_schema.condition_occurrence co
  JOIN @cdm_database_schema.visit_occurrence vo
    ON vo.visit_occurrence_id = co.visit_occurrence_id
  WHERE co.condition_concept_id = @target_concept_id
    AND co.condition_start_date IS NOT NULL
    AND vo.visit_concept_id = @outpatient_visit_concept_id
)
SELECT
  subject_id,
  cohort_start_date,
  cohort_start_date AS cohort_end_date
FROM qualifying_visits
WHERE rn = 1;

DROP TABLE IF EXISTS @cohort_database_schema.outcome_cohort;
CREATE TABLE @cohort_database_schema.outcome_cohort AS
SELECT
  co.person_id AS subject_id,
  co.condition_start_date AS cohort_start_date,
  co.condition_start_date AS cohort_end_date
FROM @cdm_database_schema.condition_occurrence co
JOIN @cdm_database_schema.visit_occurrence vo
  ON vo.visit_occurrence_id = co.visit_occurrence_id
WHERE co.condition_concept_id = @outcome_concept_id
  AND co.condition_start_date IS NOT NULL
  AND vo.visit_concept_id = @outpatient_visit_concept_id;

DROP TABLE IF EXISTS @cohort_database_schema.@cohort_table;
CREATE TABLE @cohort_database_schema.@cohort_table AS
SELECT
  @target_cohort_id AS cohort_definition_id,
  subject_id,
  cohort_start_date,
  cohort_end_date
FROM @cohort_database_schema.target_cohort
UNION ALL
SELECT
  @outcome_cohort_id AS cohort_definition_id,
  subject_id,
  cohort_start_date,
  cohort_end_date
FROM @cohort_database_schema.outcome_cohort;
"

rendered_sql <- SqlRender::render(
  sql,
  cohort_database_schema = cohort_database_schema,
  cdm_database_schema = cdm_database_schema,
  cohort_table = cohort_table,
  target_concept_id = target_concept_id,
  outcome_concept_id = outcome_concept_id,
  outpatient_visit_concept_id = outpatient_visit_concept_id,
  target_cohort_id = target_cohort_id,
  outcome_cohort_id = outcome_cohort_id
)

translated_sql <- SqlRender::translate(rendered_sql, targetDialect = dbms)
DatabaseConnector::executeSql(conn, translated_sql, progressBar = FALSE, reportOverallTime = TRUE)

target_count <- DatabaseConnector::querySql(
  conn,
  paste0("select count(*) as n from ", cohort_database_schema, ".target_cohort;")
)
outcome_count <- DatabaseConnector::querySql(
  conn,
  paste0("select count(*) as n from ", cohort_database_schema, ".outcome_cohort;")
)

print(target_count)
print(outcome_count)

if (target_count$n[1] == 0) {
  stop("Target cohort is empty. Pick a different target concept or relax the visit filter.")
}

if (outcome_count$n[1] == 0) {
  stop("Outcome cohort is empty. Pick a different outcome concept or relax the visit filter.")
}

database_details <- PatientLevelPrediction::createDatabaseDetails(
  connectionDetails = connection_details,
  cdmDatabaseSchema = cdm_database_schema,
  cdmDatabaseName = get_env_or("PLP_CDM_DATABASE_NAME", cdm_database_schema),
  cohortDatabaseSchema = cohort_database_schema,
  cohortTable = cohort_table,
  outcomeDatabaseSchema = cohort_database_schema,
  outcomeTable = cohort_table,
  targetId = target_cohort_id,
  outcomeIds = outcome_cohort_id,
  cdmVersion = 5
)

covariate_settings <- FeatureExtraction::createCovariateSettings(
  useDemographicsGender = TRUE,
  useDemographicsAge = TRUE,
  useConditionGroupEraLongTerm = TRUE,
  useConditionGroupEraAnyTimePrior = TRUE,
  useDrugGroupEraLongTerm = TRUE,
  useDrugGroupEraAnyTimePrior = TRUE,
  useVisitConceptCountLongTerm = TRUE,
  longTermStartDays = -lookback_days,
  endDays = -1
)

population_settings <- PatientLevelPrediction::createStudyPopulationSettings(
  riskWindowStart = risk_window_start,
  startAnchor = "cohort start",
  riskWindowEnd = risk_window_end,
  endAnchor = "cohort start",
  firstExposureOnly = TRUE,
  removeSubjectsWithPriorOutcome = FALSE,
  priorOutcomeLookback = 99999,
  requireTimeAtRisk = FALSE,
  minTimeAtRisk = 1
)

model_settings <- PatientLevelPrediction::setLassoLogisticRegression()

split_settings <- PatientLevelPrediction::createDefaultSplitSetting(
  type = "stratified",
  testFraction = 0.25,
  trainFraction = 0.75,
  splitSeed = 123,
  nfold = 3
)

plp_data <- PatientLevelPrediction::getPlpData(
  databaseDetails = database_details,
  covariateSettings = covariate_settings
)

plp_result <- PatientLevelPrediction::runPlp(
  plpData = plp_data,
  outcomeId = outcome_cohort_id,
  analysisId = get_env_or("PLP_ANALYSIS_ID", "p-plp-rstudio-current-setup"),
  analysisName = get_env_or("PLP_ANALYSIS_NAME", "P-PLP current setup reproduced in R"),
  populationSettings = population_settings,
  splitSettings = split_settings,
  modelSettings = model_settings,
  saveDirectory = get_env_or("PLP_OUTPUT_DIR", file.path(project_root, "plp-output"))
)

print("PLP run completed.")
print(plp_result$performanceEvaluation$evaluationStatistics)
if (requireNamespace("OhdsiShinyModules", quietly = TRUE)) {
  PatientLevelPrediction::viewPlp(plp_result)
} else {
  message(
    "Skipping viewPlp(): optional package 'OhdsiShinyModules' is not installed ",
    "or not available for this R version."
  )
}
