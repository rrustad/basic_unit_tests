import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql.types import *

def get_hospital_encounters(encounters):
  return (
    encounters
    .filter(col('encounter_type').isin(['urgentcare', 'emergency', 'inpatient']))
  )

def classify_overnight_los(encounters):
  return(
    encounters
    # Filter out any encounters where the patient hasn't been discharged
    .filter(col('stop').isNotNull())
    # Calculate How many overnight lenght of stays there were
    # .withColumn('overnight_los', f.dayofyear(col('stop')) - f.dayofyear(col('start')))
    .withColumn('start_epoc', f.floor(col('start').cast(DoubleType()) / (60*60*24)))
    .withColumn('stop_epoc', f.floor(col('stop').cast(DoubleType()) / (60*60*24)))

    .withColumn('overnight_los', col('stop_epoc') - col('start_epoc'))
    # Classify Length of Stay
    .withColumn(
      'los_type',
      f.when(col('overnight_los') < 1, 'same-day')
      .when(col('overnight_los') < 2, 'overnight')
      .otherwise(f.lit('multi-day'))
      )
    .drop('overnight_los')
    .drop('start_epoc')
    .drop('stop_epoc')
  )

  