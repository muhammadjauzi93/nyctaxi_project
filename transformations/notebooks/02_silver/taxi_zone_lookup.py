# Databricks notebook source
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType, StringType

# COMMAND ----------

# Read the taxi zone lookup CSV (with header) into a Dataframe
df = spark.read.\
    format("csv").\
    options(header='true', inferSchema='true').\
    load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")


# COMMAND ----------

# Select and cast columns, add effective_date and end_date columns
df = df.select(
        col("LocationID").cast(IntegerType()).alias("location_id"),
        col("Borough").alias("borough"),
        col("Zone").alias("zone"),
        col("service_zone"),
        current_timestamp().alias("effective_date"),
        lit(None).cast(TimestampType()).alias("end_date")
    )

# COMMAND ----------

# Load SCD2 Delta table
dt = DeltaTable.forName(spark, "nyctaxi.02_silver.taxi_zone_lookup")

# Maintain fixed end timestamp
end_timestamp = datetime.now()

# Pass 1: Close any active rows whose tracked attribute changed
dt.alias("t").\
    merge(
        source = df.alias("s"),
        condition = """
        t.location_id = s.location_id 
        AND t.end_date is NULL
        AND (
            t.borough != s.borough 
            OR t.zone != s.zone 
            OR t.service_zone != s.service_zone
        )
        """
    ).\
    whenMatchedUpdate(
        set = {"t.end_date": lit(end_timestamp).cast(TimestampType())}
    ).\
    execute()

# COMMAND ----------

# Pass 2: Insert new versions

# get list of IDs that have been closed
insert_id_list = [row.location_id for row in dt.toDF().filter(f"end_date = '{end_timestamp}'").select("location_id").collect()]

# If the list if empty, dont insert anything
if len(insert_id_list) == 0:
    print("No updated records to insert")
else:
    dt.alias("t").\
        merge(
            source = df.alias("s"),
            condition = f"s.location_id NOT IN ({','.join(map(str, insert_id_list))})"
        ).\
        whenNotMatchedInsert(
            values={
                "t.location_id":"s.location_id",
                "t.borough": "s.borough",
                "t.zone": "s.zone",
                "t.service_zone":"s.service_zone",
                "t.effective_date": "s.effective_date",
                "t.end_date": lit(None).cast(TimestampType())
            }    
        ).execute()


# COMMAND ----------

# Pass 3: Insert brand new keys (no historical row in target)

dt.alias("t").\
        merge(
            source = df.alias("s"),
            condition = "s.location_id = t.location_id"
        ).\
        whenNotMatchedInsert(
            values={
                "t.location_id":"s.location_id",
                "t.borough": "s.borough",
                "t.zone": "s.zone",
                "t.service_zone":"s.service_zone",
                "t.effective_date": "s.effective_date",
                "t.end_date": lit(None).cast(TimestampType())
            }    
        ).execute()