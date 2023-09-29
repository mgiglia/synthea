-- Databricks notebook source
use catalog lakehouse_dev

-- COMMAND ----------

use schema patient

-- COMMAND ----------

show volumes

-- COMMAND ----------

list '/Volumes/lakehouse_dev/patient/synthea/'

-- COMMAND ----------

list "/Volumes/lakehouse_dev/patient/synthea/csv/"

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC cat "/Volumes/lakehouse_dev/patient/synthea/csv/allergies.csv"

-- COMMAND ----------

SELECT * FROM csv.`/Volumes/lakehouse_dev/patient/synthea/csv/allergies.csv` 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC # read CSV file into a Spark dataframe
-- MAGIC df = spark.read.format("csv") \
-- MAGIC       .option("header", "true") \
-- MAGIC       .option("inferSchema", "true") \
-- MAGIC       .option("mode", "PERMISSIVE") \
-- MAGIC       .load("/Volumes/lakehouse_dev/patient/synthea/csv/allergies.csv")
-- MAGIC       
-- MAGIC # display first 10 rows of dataframe
-- MAGIC display(df.limit(10))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta")\
-- MAGIC   .mode("overwrite")\
-- MAGIC   .save("/Volumes/lakehouse_dev/patient/synthea/delta/allergies")

-- COMMAND ----------

select * from delta.`/Volumes/lakehouse_dev/patient/synthea/delta/allergies`
