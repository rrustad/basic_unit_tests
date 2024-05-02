# Databricks notebook source
import pyspark.testing
from pyspark.testing.utils import assertDataFrameEqual
from datetime import datetime
from encounters_etl import get_hospital_encounters, classify_overnight_los

# COMMAND ----------

def test_get_hospital_encounters():
  in_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'ambulatory'],
      [2, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'wellness'],
      [3, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'outpatient'],
      [4, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'urgentcare'],
      [5, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'emergency'],
      [6, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'inpatient'],
      [7, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'home'],
      [8, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'hospice'],
      [9, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'snf'],
      [10, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'virtual'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string")
  )
  out_df = (
    spark.createDataFrame([
      [4, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'urgentcare'],
      [5, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'emergency'],
      [6, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'inpatient'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string")
  )

  assertDataFrameEqual(get_hospital_encounters(in_df),out_df)

# COMMAND ----------

test_get_hospital_encounters()

# COMMAND ----------

datetime(2023,1,1,0,0,0)

# COMMAND ----------

def test_add_col_los_tyoe():
  in_df = (
    spark.createDataFrame([
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string")
  )
  out_df = (
    spark.createDataFrame([
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string, los_type string")
  )
  assertDataFrameEqual(classify_overnight_los(in_df),out_df)
test_add_col_los_tyoe()

# COMMAND ----------

# class TestClassifyOvernightLOS:
def test_null_filter():
  in_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), None, 'ambulatory'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string")
  )
  out_df = (
    spark.createDataFrame([
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string, los_type string")
  )
  assertDataFrameEqual(classify_overnight_los(in_df),out_df)
test_null_filter()

# COMMAND ----------

# class TestClassifyOvernightLOS:
def test_classify_sameday():
  in_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,1,0,1), 'ambulatory'],
      [2, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,1,23,59,59), 'ambulatory'],
      [3, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,23,59,59), 'ambulatory'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string")
  )
  out_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,1,0,1), 'ambulatory', 'same-day'],
      [2, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,1,23,59,59), 'ambulatory', 'same-day'],
      [3, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,23,59,59), 'ambulatory', 'same-day'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string, los_type string")
  )
  assertDataFrameEqual(classify_overnight_los(in_df),out_df)
test_classify_sameday()

# COMMAND ----------

def test_classify_overnight():
  in_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,2,1,0,0), 'ambulatory'],
      [2, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,2,1,0,0), 'ambulatory'],
      [3, 1.0, datetime(2023,1,1,23,59,59), datetime(2023,1,2,23,59,59), 'ambulatory'],
      [4, 1.0, datetime(2023,12,31,23,59,59), datetime(2024,1,1,0,0,0), 'ambulatory'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string")
  )
  out_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,2,1,0,0), 'ambulatory', 'overnight'],
      [2, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,2,1,0,0), 'ambulatory', 'overnight'],
      [3, 1.0, datetime(2023,1,1,23,59,59), datetime(2023,1,2,23,59,59), 'ambulatory', 'overnight'],
      [4, 1.0, datetime(2023,12,31,23,59,59), datetime(2024,1,1,0,0,0), 'ambulatory', 'overnight'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string, los_type string")
  )
  assertDataFrameEqual(classify_overnight_los(in_df),out_df)
test_classify_overnight()

# COMMAND ----------

def test_classify_multiday():
  in_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,3,1,0,0), 'ambulatory'],
      [2, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,3,1,0,0), 'ambulatory'],
      [3, 1.0, datetime(2023,1,1,23,59,59), datetime(2023,1,3,23,59,59), 'ambulatory'],
      [4, 1.0, datetime(2023,12,31,23,59,59), datetime(2024,1,2,0,0,0), 'ambulatory'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string")
  )
  out_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,3,1,0,0), 'ambulatory', 'multi-day'],
      [2, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,3,1,0,0), 'ambulatory', 'multi-day'],
      [3, 1.0, datetime(2023,1,1,23,59,59), datetime(2023,1,3,23,59,59), 'ambulatory', 'multi-day'],
      [4, 1.0, datetime(2023,12,31,23,59,59), datetime(2024,1,2,0,0,0), 'ambulatory', 'multi-day'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string, los_type string")
  )
  assertDataFrameEqual(classify_overnight_los(in_df),out_df)
test_classify_multiday()

# COMMAND ----------

def test_classify_all():
  in_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,1,0,1), 'ambulatory'],
      [2, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,1,23,59,59), 'ambulatory'],
      [3, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,23,59,59), 'ambulatory'],
      [4, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,2,1,0,0), 'ambulatory'],
      [5, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,2,1,0,0), 'ambulatory'],
      [6, 1.0, datetime(2023,1,1,23,59,59), datetime(2023,1,2,23,59,59), 'ambulatory'],
      [7, 1.0, datetime(2023,12,31,23,59,59), datetime(2024,1,1,0,0,0), 'ambulatory'],
      [8, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,3,1,0,0), 'ambulatory'],
      [9, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,3,1,0,0), 'ambulatory'],
      [10, 1.0, datetime(2023,1,1,23,59,59), datetime(2023,1,3,23,59,59), 'ambulatory'],
      [11, 1.0, datetime(2023,12,31,23,59,59), datetime(2024,1,2,0,0,0), 'ambulatory'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string")
  )
  out_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,1,0,1), 'ambulatory', 'same-day'],
      [2, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,1,23,59,59), 'ambulatory', 'same-day'],
      [3, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,23,59,59), 'ambulatory', 'same-day'],
      [4, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,2,1,0,0), 'ambulatory', 'overnight'],
      [5, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,2,1,0,0), 'ambulatory', 'overnight'],
      [6, 1.0, datetime(2023,1,1,23,59,59), datetime(2023,1,2,23,59,59), 'ambulatory', 'overnight'],
      [7, 1.0, datetime(2023,12,31,23,59,59), datetime(2024,1,1,0,0,0), 'ambulatory', 'overnight'],
      [8, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,3,1,0,0), 'ambulatory', 'multi-day'],
      [9, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,3,1,0,0), 'ambulatory', 'multi-day'],
      [10, 1.0, datetime(2023,1,1,23,59,59), datetime(2023,1,3,23,59,59), 'ambulatory', 'multi-day'],
      [11, 1.0, datetime(2023,12,31,23,59,59), datetime(2024,1,2,0,0,0), 'ambulatory', 'multi-day'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string, los_type string")
  )
  assertDataFrameEqual(classify_overnight_los(in_df),out_df)
test_classify_all()

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %sh ls /tmp

# COMMAND ----------

# MAGIC %sh pytest cache_dir="/tmp/cache"

# COMMAND ----------

def test_classify_multiday():
  in_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,3,1,0,0), 'ambulatory'],
      [2, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,3,1,0,0), 'ambulatory'],
      [3, 1.0, datetime(2023,1,1,23,59,59), datetime(2023,1,3,23,59,59), 'ambulatory'],
      [4, 1.0, datetime(2023,12,31,23,59,59), datetime(2024,1,2,0,0,0), 'ambulatory'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string")
  )
  out_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,3,1,0,0), 'ambulatory', 'multi-day'],
      [2, 1.0, datetime(2023,1,1,23,59,58), datetime(2023,1,3,1,0,0), 'ambulatory', 'multi-day'],
      [3, 1.0, datetime(2023,1,1,23,59,59), datetime(2023,1,3,23,59,59), 'ambulatory', 'multi-day'],
      [4, 1.0, datetime(2023,12,31,23,59,59), datetime(2024,1,2,0,0,0), 'ambulatory', 'multi-day'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string, los_type string")
  )
  assertDataFrameEqual(classify_overnight_los(in_df),out_df)
test_classify_multiday()

# COMMAND ----------

def test_get_hospital_encounters():
  in_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'ambulatory'],
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'wellness'],
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'outpatient'],
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'urgentcare'],
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'emergency'],
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'inpatient'],
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'home'],
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'hospice'],
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'snf'],
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'virtual'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string")
  )
  out_df = (
    spark.createDataFrame([
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'urgentcare'],
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'emergency'],
      [1, 1.0, datetime(2023,1,1,0,0,0), datetime(2023,1,1,0,0,0), 'inpatient'],
    ],
    schema="id long, dept_id double, start timestamp, stop timestamp, encounter_type string")
  )

  assertDataFrameEqual(get_hospital_encounters(in_df),out_df)

# COMMAND ----------


