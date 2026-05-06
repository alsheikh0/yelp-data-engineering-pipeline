# Databricks notebook source
# Title: silver_checkins
# ─────────────────────────────────────────────────────────────────────────────
# checkin.json → silver_checkins (Delta)
#
# The nastiest field in the whole Yelp dataset:
#   { "business_id": "abc123",
#     "date": "2016-04-26 19:49:16, 2017-08-30 18:36:57, 2021-10-15 02:45:18" }
#
# The `date` field is a single comma-separated string of ALL timestamps
# a business ever received a checkin. One Bronze row can represent
# thousands of individual checkin events.
#
# Steps:
#   1. Split the date string on "," → array of timestamp strings
#   2. Explode → one row per individual checkin event
#   3. Parse to proper timestamp type
#   4. Derive time-dimension columns (hour, day_of_week, time_of_day)
#   5. Write as Delta partitioned by year+month
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
dbutils.widgets.text("storage_account", "yelpstorage", "Storage Account")

STORAGE    = dbutils.widgets.get("storage_account")
BRONZE     = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/yelp/checkin.json"
SILVER     = f"abfss://silver@{STORAGE}.dfs.core.windows.net/checkins/"
QUARANTINE = f"abfss://silver@{STORAGE}.dfs.core.windows.net/rejected/checkins/"

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, explode, trim, to_timestamp, year, month,
    hour, dayofweek, when, lit, size, current_timestamp,
    count, coalesce
)
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("silver_checkins") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------
# STEP 1 — Read raw JSON (schema: business_id STRING, date STRING)
df_raw = spark.read.json(BRONZE)
bronze_business_count = df_raw.count()
print(f"Raw checkin rows (one per business): {bronze_business_count:,}")

# COMMAND ----------
# STEP 2 — Split the comma-separated date string into an array
# Then capture the total checkin count per business BEFORE exploding
# so we can validate row counts after the explode
df_split = df_raw \
    .withColumn("checkin_ts_array",
        split(col("date"), ",")) \
    .withColumn("total_checkins_for_business",
        size(col("checkin_ts_array")))

expected_total = df_split.agg({"total_checkins_for_business": "sum"}).collect()[0][0]
print(f"Expected total checkin events after explode: {expected_total:,}")

# COMMAND ----------
# STEP 3 — Explode: one row per individual checkin event
df_exploded = df_split.select(
    col("business_id"),
    col("total_checkins_for_business"),
    trim(explode(col("checkin_ts_array"))).alias("checkin_ts_raw")
)
actual_exploded = df_exploded.count()
print(f"Actual rows after explode: {actual_exploded:,}")

if actual_exploded != expected_total:
    raise ValueError(
        f"Explode count mismatch! "
        f"Expected {expected_total:,}, got {actual_exploded:,}"
    )

# COMMAND ----------
# STEP 4 — Parse timestamps + quarantine unparseable values
df_parsed = df_exploded \
    .withColumn("checkin_at",
        to_timestamp(col("checkin_ts_raw"), "yyyy-MM-dd HH:mm:ss"))

df_bad_ts  = df_parsed.filter(col("checkin_at").isNull())
df_good_ts = df_parsed.filter(col("checkin_at").isNotNull())

(df_bad_ts.write.format("delta").mode("append")
    .option("overwriteSchema","true").save(QUARANTINE))
print(f"Bad timestamps quarantined: {df_bad_ts.count():,}")

# COMMAND ----------
# STEP 5 — Derive time-dimension columns
df_silver = df_good_ts \
    .withColumn("checkin_year",  year("checkin_at")) \
    .withColumn("checkin_month", month("checkin_at")) \
    .withColumn("checkin_hour",  hour("checkin_at")) \
    .withColumn("checkin_dow",   dayofweek("checkin_at")) \
    .withColumn("checkin_date",  col("checkin_at").cast("date")) \
    .withColumn("is_weekend",
        col("checkin_dow").isin([1, 7]).cast("boolean")) \
    .withColumn("time_of_day",
        when(col("checkin_hour").between(6,  11), "morning")
       .when(col("checkin_hour").between(12, 16), "afternoon")
       .when(col("checkin_hour").between(17, 21), "evening")
       .otherwise("night")) \
    .withColumn("is_peak_hour",
        col("checkin_hour").between(11, 13)
        | col("checkin_hour").between(18, 21)) \
    .withColumn("loaded_at", current_timestamp()) \
    .drop("checkin_ts_raw")

# COMMAND ----------
# STEP 6 — Write Silver partitioned by year + month
(df_silver
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .partitionBy("checkin_year", "checkin_month")
    .save(SILVER))

silver_count = spark.read.format("delta").load(SILVER).count()
print(f"Silver checkins written: {silver_count:,}")

# COMMAND ----------
spark.sql(
    f"OPTIMIZE delta.`{SILVER}` "
    f"ZORDER BY (business_id, checkin_at)"
)
print("OPTIMIZE complete.")
