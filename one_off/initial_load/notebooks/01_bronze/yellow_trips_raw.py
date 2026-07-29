# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Why mergeSchema? Because 2024 & 2025 files have different schema
df = spark.read.option("mergeSchema", "true").parquet("/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/*")

df = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

spark.read.table("nyctaxi.01_bronze.yellow_trips_raw").display()