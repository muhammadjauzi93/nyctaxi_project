# Databricks notebook source
import sys
import os

# Go up two directories to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), '../..'))

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.utils.date_utils import get_month_start_n_months_ago
from pyspark.sql.functions import date_format, col

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = get_month_start_n_months_ago(2)

# COMMAND ----------

# Read yellow_trips_enriched table from nyctaxi.02_silver and filter only to include data later than two months ago
df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched").filter(f"tpep_pickup_datetime >= '{two_months_ago_start}'")

# COMMAND ----------

# Add year_month column, formatted as yyyy-MM
df = df.withColumn("year_month", date_format("tpep_pickup_datetime", "yyyy-MM"))

# COMMAND ----------

# Write the yellow_trips data in JSON format to the External Table "yellow_trips_export"
df.write.\
    option("path", "abfss://nyctaxi-yellow@nyctaxistoragejauzi.dfs.core.windows.net/yellow_trips_export/").\
    format("json").\
    mode("append").\
    partitionBy("vendor","year_month").\
    saveAsTable("nyctaxi.04_export.yellow_trips_export")