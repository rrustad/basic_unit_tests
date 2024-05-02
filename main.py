# Databricks notebook source
# MAGIC %run ./_setup

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.functions import col
from encounters_etl import *

# COMMAND ----------

hospital_encounters = get_hospital_encounters(spark.table('encounters'))

# COMMAND ----------

# hospital_encounters.display()

# COMMAND ----------

overnight_los = classify_overnight_los(hospital_encounters)

# COMMAND ----------

overnight_los.groupby('dept_id','los_type').agg(f.sum(f.lit(1))).display()

# COMMAND ----------


