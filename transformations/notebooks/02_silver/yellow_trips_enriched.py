# Databricks notebook source
from datetime import date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = date.today().replace(day=1) - relativedelta(months=2)

# COMMAND ----------

# Load cleansed trip data from Silver layer
# and filter only to include trips with a pickup datetime
# later than the start date from two months ago
df_trips = spark.read.table("nyctaxi.02_silver.yellow_trips_cleansed").\
    filter(f"tpep_pickup_datetime > '{two_months_ago_start}'")

# Load taxi zone lookup data from Silver layer
df_zones = spark.read.table("nyctaxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

df_join1 = df_trips.\
    join(df_zones, df_trips.pu_location_id == df_zones.location_id, "left") \
    .select(
        df_trips.vendor,
        df_trips.tpep_pickup_datetime,
        df_trips.tpep_dropoff_datetime,
        df_trips.trip_duration.alias("trip_duration_mins"),
        df_trips.passenger_count,
        df_trips.trip_distance,
        df_trips.rate_type,
        df_trips.store_and_fwd_flag,
        df_zones.borough.alias("pu_borough"),
        df_zones.zone.alias("pu_zone"),
        df_trips.do_location_id,
        df_trips.payment_type,
        df_trips.fare_amount,
        df_trips.extra,
        df_trips.mta_tax,
        df_trips.tip_amount,
        df_trips.tolls_amount,
        df_trips.improvement_surcharge,
        df_trips.total_amount,
        df_trips.congestion_surcharge,  
        df_trips.airport_fee,
        df_trips.cbd_congestion_fee,
        df_trips.processed_timestamp
    )

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql import functions as F

df_join2 = df_join1.alias("j1").\
    join(df_zones.alias("z2"), F.col("j1.do_location_id") == F.col("z2.location_id"), "left").\
    select(
        F.col("j1.vendor"),
        F.col("j1.tpep_pickup_datetime"),
        F.col("j1.tpep_dropoff_datetime"),
        F.col("j1.trip_duration_mins"),
        F.col("j1.passenger_count"),
        F.col("j1.trip_distance"),
        F.col("j1.rate_type"),
        F.col("j1.store_and_fwd_flag"),
        F.col("j1.pu_borough"),
        F.col("z2.borough").alias("do_borough"),
        F.col("j1.pu_zone"),
        F.col("z2.zone").alias("do_zone"),
        F.col("j1.payment_type"),
        F.col("j1.fare_amount"),
        F.col("j1.extra"),
        F.col("j1.mta_tax"),
        F.col("j1.tip_amount"),
        F.col("j1.tolls_amount"),
        F.col("j1.improvement_surcharge"),
        F.col("j1.total_amount"),
        F.col("j1.congestion_surcharge"),
        F.col("j1.airport_fee"),
        F.col("j1.cbd_congestion_fee"),
        F.col("j1.processed_timestamp")
    )


# COMMAND ----------

df_join2.write.mode("append").saveAsTable("nyctaxi.02_silver.yellow_trips_enriched")