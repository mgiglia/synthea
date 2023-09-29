-- Databricks notebook source
use catalog lakehouse_dev

-- COMMAND ----------

use schema patient

-- COMMAND ----------

show volumes

-- COMMAND ----------

list "/Volumes/lakehouse_dev/patient/synthea/"

-- COMMAND ----------

list "/Volumes/lakehouse_dev/patient/synthea/csv"

-- COMMAND ----------

select * from csv.`/Volumes/lakehouse_dev/patient/synthea/csv/careplans.csv`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC datafile = "/Volumes/lakehouse_dev/patient/synthea/csv/careplans.csv"
-- MAGIC df = spark.read.format("csv") \
-- MAGIC           .option("header", "true") \
-- MAGIC           .option("inferSchema", "true") \
-- MAGIC           .load(datafile)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)
