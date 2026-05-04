library(DatabaseConnector)
Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "C:/jdbc")

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server = "localhost/synpuf100kaws",
                                                                user = "postgres",
                                                                password = "",
                                                                port = 5432 )

cdmDatabaseSchema <- "public"
cohortsDatabaseSchema <- "public"
cdmVersion <- "5"


library(FeatureExtraction)
covariateSettings <- createCovariateSettings(
  useDemographicsGender = TRUE,
  useDemographicsAge = TRUE,
  useConditionOccurrenceAnyTimePrior = TRUE,
  useDrugExposureAnyTimePrior = TRUE,
  useProcedureOccurrenceAnyTimePrior = TRUE,
  useVisitConceptCountLongTerm = TRUE,
  longTermStartDays = -365,
  endDays = -1
)


library(PatientLevelPrediction)
databaseDetails <- createDatabaseDetails(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cdmDatabaseName = "",
  cohortDatabaseSchema = cohortsDatabaseSchema,
  cohortTable = "cohort",
  targetId = 1,
  outcomeDatabaseSchema = cohortsDatabaseSchema,
  outcomeTable = "cohort",
  outcomeIds = 2,
  cdmVersion = 5
)

# here you can define whether you want to sample the target cohort and add any
# restrictions based on minimum prior observation, index date restrictions
# or restricting to first index date (if people can be in target cohort multiple times)
restrictPlpDataSettings <- createRestrictPlpDataSettings()
plpData <- getPlpData(
  databaseDetails = databaseDetails,
  covariateSettings = covariateSettings,
  restrictPlpDataSettings = restrictPlpDataSettings
)

plpData
summary(plpData)
savePlpData(plpData, "stroke_in_af_data")

populationSettings <- createStudyPopulationSettings(
  firstExposureOnly = FALSE,
  removeSubjectsWithPriorOutcome = TRUE,
  priorOutcomeLookback = 1,
  riskWindowStart = 0,
  riskWindowEnd = 365,
  startAnchor = "cohort start",
  endAnchor = "cohort start",
  minTimeAtRisk = 364,
  requireTimeAtRisk = TRUE,
  includeAllOutcomes = TRUE
)

splitSettings <- createDefaultSplitSetting(
  trainFraction = 0.75,
  testFraction = 0.25,
  type = "stratified",
  nfold = 2,
  splitSeed = 1234
)

sampleSettings <- createSampleSettings()


preprocessSettingsSettings <- createPreprocessSettings(
  minFraction = 0.001,
  normalize = TRUE,
  removeRedundancy = TRUE
)

executeSettings = createExecuteSettings(
  runSplitData = TRUE,
  runSampleData = TRUE,
  runPreprocessData = TRUE,
  runModelDevelopment = TRUE,
  runCovariateSummary = TRUE
)



featureEngineeringSettings <- createFeatureEngineeringSettings()

population <- createStudyPopulation(
  plpData = plpData,
  outcomeId = 2,
  populationSettings = populationSettings
)

population
table(population$outcomeCount)
nrow(population)


modelList <- list(
  lasso = setLassoLogisticRegression(),
  rf = setRandomForest(ntrees = list(200))
)

results <- list()

for(name in names(modelList)) {
  results[[name]] <- runPlp(
    plpData = plpData,
    outcomeId = 2,
    analysisId = name,
    analysisName = paste("Model:", name),
    populationSettings = populationSettings,
    splitSettings = splitSettings,
    sampleSettings = sampleSettings,
    preprocessSettings = preprocessSettings,
    featureEngineeringSettings = featureEngineeringSettings,
    modelSettings = modelList[[name]],
    executeSettings = executeSettings
  )
}
summary(plpData)


for(name in names(results)) {
  
  cat("\nModel:", name, "\n")
  
  pred <- results[[name]]$prediction
  
  threshold <- mean(pred$outcomeCount)
  pred$predictedClass <- ifelse(pred$value > threshold, 1, 0)
  
  print(table(Predicted = pred$predictedClass,
              Actual = pred$outcomeCount))
}
