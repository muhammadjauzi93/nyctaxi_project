# Databricks notebook source
from pyspark.sql.functions import col, to_date, to_timestamp,count,avg,max,min,sum,round
from dateutil.relativedelta import relativedelta
from datetime import date

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = date.today().replace(day=1) - relativedelta(months=2)

# COMMAND ----------

# Load enriched trip dataset
# and filter only to include trips with a pickup datetime
# later than the start date from two months ago
df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched").\
    filter(f"tpep_pickup_datetime > '{two_months_ago_start}'")


# COMMAND ----------

df = df.withColumn("pickup_date", to_date("tpep_pickup_datetime"))

# COMMAND ----------

# Aggregate daily trip metrics
df = df.groupBy("pickup_date").\
    agg(
        count("*").alias("total_trips"),  # Total number of trips per day
        round(avg("passenger_count"),1).alias("avg_passengers_per_trip"),  # Average passengers per trip
        round(avg("trip_distance"),1).alias("avg_distance_per_trip"),  # Average trip distance
        round(avg("fare_amount"),2).alias("avg_fare_per_trip"),  # Average fare per trip
        max("fare_amount").alias("max_fare"),  # Maximum fare per day
        min("fare_amount").alias("min_fare"),  # Minimum fare per day
        round(sum("total_amount"),2).alias("total_revenue")  # Total revenue per day
    )

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi.03_gold.daily_trip_summary")