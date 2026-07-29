# Databricks notebook source
from pyspark.sql.functions import col, when, timestamp_diff
from datetime import date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = date.today().replace(day=1) - relativedelta(months=2)

# Get the first day of the month one months ago
one_month_ago_start = date.today().replace(day=1) - relativedelta(months=1)

# COMMAND ----------

df = spark.read.table("nyctaxi.01_bronze.yellow_trips_raw").filter((col("tpep_pickup_datetime") >= two_months_ago_start) & (col("tpep_pickup_datetime") < one_month_ago_start))

# COMMAND ----------

df = df.select(
    # Map VendorID to vendor name
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
    .when(col("VendorID") == 2, "Curb Mobility, LLC")
    .when(col("VendorID") == 6, "Myle Technologies Inc")
    .when(col("VendorID") == 7, "Helix")
    .otherwise("Unknown")
    .alias("vendor"),

    # Pickup and dropoff timestamps
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",

    # Calculate trip duration in minutes
    timestamp_diff('MINUTE',col("tpep_pickup_datetime"), col("tpep_dropoff_datetime")).alias("trip_duration"),

    # Passenger count and trip distance
    "passenger_count",
    "trip_distance",

    # Map RatecodeID to rate type
    when(col("RatecodeID") == 1, "Standard rate")
    .when(col("RatecodeID") == 2, "JFK")
    .when(col("RatecodeID") == 3, "Newark")
    .when(col("RatecodeID") == 4, "Nassau or Westchester")
    .when(col("RatecodeID") == 5, "Negotiated fare")
    .when(col("RatecodeID") == 6, "Group ride")
    .otherwise("Unknown")
    .alias("rate_type"),

    # Store and forward flag
    "store_and_fwd_flag",

    # Pickup and dropoff location IDs
    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),

    # Map payment_type to payment description
    when(col("payment_type") == 0, "Flex Fare trip")
    .when(col("payment_type") == 1, "Credit Card")
    .when(col("payment_type") == 2, "Cash")
    .when(col("payment_type") == 3, "No charge")
    .when(col("payment_type") == 4, "Dispute")
    .when(col("payment_type") == 5, "Unknown")
    .when(col("payment_type") == 6, "Voided trip")
    .otherwise("Unknown")
    .alias("payment_type"),

    # Fare and fee columns
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    col("Airport_fee").alias("airport_fee"),
    "cbd_congestion_fee",
    "processed_timestamp"
)

# COMMAND ----------

# Write cleansed datato a Unity Catalog managed Delta table in silver schema
df.write.mode("append").saveAsTable("nyctaxi.02_silver.yellow_trips_cleansed")