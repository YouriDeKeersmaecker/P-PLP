from .cohorts import *
from .db import *
from .feature_engineering import *
from .modeling import *


def run_pipeline():
    engine = get_engine()
    generate_target_cohort(engine, 40481087) 
    generate_outcome_cohort(engine, 40481087)  
    generate_labels_time_at_risk(engine)

if __name__ == "__main__":
    run_pipeline()