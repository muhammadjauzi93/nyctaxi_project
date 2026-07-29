# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which vendor makes the most revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT vendor, SUM(total_amount) AS total_revenue 
# MAGIC FROM nyctaxi.02_silver.yellow_trips_enriched
# MAGIC GROUP BY vendor
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is the most popular pickup borough

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pu_borough, COUNT(*) as total_trips
# MAGIC FROM nyctaxi.02_silver.yellow_trips_enriched
# MAGIC GROUP BY pu_borough
# MAGIC ORDER BY total_trips DESC
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is the common journey (borough to borough)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT pu_borough,do_borough, COUNT(*) as total_trips
# MAGIC FROM nyctaxi.02_silver.yellow_trips_enriched
# MAGIC GROUP BY pu_borough,do_borough
# MAGIC ORDER BY total_trips DESC
# MAGIC LIMIT 1

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

df.groupBy(F.concat("pu_borough", F.lit("->"), "do_borough").alias("journey"))\
    .agg(F.count("*").alias("total_trips"))\
    .orderBy("total_trips", ascending=False)\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time series chart, number of trips vs total revenue per day

# COMMAND ----------




df.groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date"))\
    .agg(
    F.count("*").alias("total_trips"),
    F.sum("total_amount").alias("total_revenue"))\
    .orderBy("pickup_date")\
    .display()