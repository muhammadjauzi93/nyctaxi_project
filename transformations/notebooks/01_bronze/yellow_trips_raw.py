# Databricks notebook source
import sys
import os

# Go up two directories to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), '../..'))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta
from datetime import date
from modules.utils.date_utils import get_target_yyyymm
from modules.transformations.metadata import add_ingestion_date

# COMMAND ----------

# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
formatted_date = get_target_yyyymm(2)

# Read all parquet file for the specified month from the landing directory into a DataFrame
df = spark.read.parquet(f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}")

# COMMAND ----------

# Add ingestion metadata to the DataFrame
df = add_ingestion_date(df)

# COMMAND ----------

# Write the processed DataFrame to the a managed Delta table in the bronze schema, appending the data
df.write.mode("append").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")