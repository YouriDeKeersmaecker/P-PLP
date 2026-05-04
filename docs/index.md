# P-PLP

P-PLP is a Python package for patient-level prediction workflows on OMOP CDM data.

It provides a few focused layers:

- `p_plp.db` for database connections and validation
- `p_plp.cohorts` for Atlas cohort loading and label generation
- `p_plp.feature_engineering` for feature dataset creation
- `p_plp.modeling` for model training and evaluation
- `p_plp.config` for simple configuration objects

Supported data sources:

- PostgreSQL OMOP databases
- DuckDB / Eunomia-style OMOP datasets

The package is currently best used step by step: connect, load cohorts, generate labels, build features, then train a model.
