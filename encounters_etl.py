import pyspark.sql.functions as f
from pyspark.sql.functions import col

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
    .withColumn('overnight_los', f.dayofyear(col('stop')) - f.dayofyear(col('start')))
    # Classify Length of Stay
    .withColumn(
      'los_type',
      f.when(col('overnight_los') < 1, 'same-day')
      .when(col('overnight_los') < 2, 'overnight')
      .otherwise(f.lit('multi-day'))
      )
    .drop('overnight_los')
  )