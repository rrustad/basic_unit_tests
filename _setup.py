# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS kp_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS kp_catalog.unit_tests;
# MAGIC USE kp_catalog.unit_tests;
# MAGIC drop table if exists encounters;

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, TimestampType
from datetime import datetime
now = datetime.now()

(
  spark.range(100000)
  .withColumn('dept_id', f.round((f.rand(seed=42))*5))
  .withColumn('start', f.round(f.rand(seed=43) * 365*24*60*60))
  .withColumn('start', (f.lit(now).cast(IntegerType()) - f.col('start')).cast(TimestampType()))
  .withColumn('len_of_stay', f.round(f.exp(f.abs(f.randn(seed=44))*4))*60)
  .withColumn('stop', (f.col('start').cast(IntegerType()) + f.col('len_of_stay')).cast(TimestampType()))
  .withColumn('stop', f.when(f.col('stop') > now, f.lit(None)).otherwise(f.col('stop')))
  .withColumn('encounter_type', f.rand(seed=45))
  .withColumn('encounter_type', 
              f.when(f.col('encounter_type') < .44, 'ambulatory')
              .when(f.col('encounter_type') < 0.70, 'wellness')
              .when(f.col('encounter_type') < 0.85, 'outpatient')
              .when(f.col('encounter_type') < 0.92, 'urgentcare')
              .when(f.col('encounter_type') < 0.94, 'emergency')
              .when(f.col('encounter_type') < 0.96, 'inpatient')
              .when(f.col('encounter_type') < 0.97, 'home')
              .when(f.col('encounter_type') < 0.98, 'hospice')
              .when(f.col('encounter_type') < 0.99, 'snf')
              .when(f.col('encounter_type') < 1.00, 'virtual')
              )
  .drop('len_of_stay')
  .write
  .mode('overwrite')
  .saveAsTable('encounters')
  )
# spark.table('encounters').display()

# COMMAND ----------

# encounters.filter(f.col('stop').isNull()).display()

# COMMAND ----------

spark.table('encounters').display()

# COMMAND ----------


