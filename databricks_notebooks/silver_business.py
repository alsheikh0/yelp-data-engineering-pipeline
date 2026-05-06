# Databricks notebook source
# Title: silver_business
# ─────────────────────────────────────────────────────────────────────────────
# business.json → silver_business (Delta, SCD Type 2 via MERGE)
#
# Real challenges in this file:
#   1. `attributes` — nested dict, ~50 keys, values are strings,
#      booleans, OR nested dicts (e.g. Ambience, GoodForMeal)
#   2. `categories`  — comma-separated string, needs exploding
#   3. `hours`       — dict of day → "HH:MM-HH:MM" strings
#   4. SCD Type 2   — businesses open/close, change ratings/categories
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
dbutils.widgets.text("storage_account", "yelpstorage", "Storage Account Name")
dbutils.widgets.dropdown("mode", "merge", ["merge", "full"], "Run Mode")

STORAGE = dbutils.widgets.get("storage_account")
MODE    = dbutils.widgets.get("mode")

BRONZE     = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/yelp/business.json"
SILVER     = f"abfss://silver@{STORAGE}.dfs.core.windows.net/business/"
SILVER_CATS= f"abfss://silver@{STORAGE}.dfs.core.windows.net/business_categories/"
QUARANTINE = f"abfss://silver@{STORAGE}.dfs.core.windows.net/rejected/business/"

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, explode, trim, when, lit, current_timestamp,
    regexp_extract, coalesce, sha2, concat_ws, size
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName(f"silver_business_{MODE}") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------
# STEP 1 — Read raw JSON
# business.json is newline-delimited JSON, one object per line
df_raw = spark.read.json(BRONZE)
raw_count = df_raw.count()
print(f"Raw business rows: {raw_count:,}")
df_raw.printSchema()

# COMMAND ----------
# STEP 2 — Flatten core scalar fields
df_core = df_raw.select(
    col("business_id"),
    col("name").alias("business_name"),
    col("address"),
    col("city"),
    col("state"),
    col("postal_code"),
    col("latitude").cast(DoubleType()),
    col("longitude").cast(DoubleType()),
    col("stars").cast(DoubleType()),
    col("review_count").cast(IntegerType()),
    col("is_open").cast(BooleanType()),
    col("categories"),      # raw comma-sep string — explode later
    col("attributes"),      # raw struct — flatten below
    col("hours"),           # raw struct — flatten below
)

# COMMAND ----------
# STEP 3 — Flatten `hours` struct
# Each field is a string like "9:0-22:0" or None.
# We extract open/close hour as integers for each day.
days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

df_hours = df_core
for day in days:
    short = day[:3].lower()
    df_hours = df_hours \
        .withColumn(f"{short}_open",
            regexp_extract(
                coalesce(col(f"hours.{day}"), lit("")),
                r"^(\d+):\d+-", 1
            ).cast(IntegerType())) \
        .withColumn(f"{short}_close",
            regexp_extract(
                coalesce(col(f"hours.{day}"), lit("")),
                r"-(\d+):\d+$", 1
            ).cast(IntegerType()))

df_hours = df_hours.drop("hours")

# COMMAND ----------
# STEP 4 — Flatten `attributes` struct
# Attributes vary wildly per business. Values can be:
#   - Plain strings: "u'free'", "True", "False"
#   - Nested JSON strings: "{'romantic': True, 'casual': False}"
# Strategy: extract high-value scalar attrs by name,
# use regex to clean Pythonic boolean strings.
df_attrs = df_hours \
    .withColumn("price_range",
        col("attributes.RestaurantsPriceRange2").cast(IntegerType())) \
    .withColumn("wifi",
        when(col("attributes.WiFi").rlike("free"), "free")
       .when(col("attributes.WiFi").rlike("paid"), "paid")
       .otherwise("none")) \
    .withColumn("outdoor_seating",
        when(col("attributes.OutdoorSeating").rlike("(?i)true"), True)
       .otherwise(False).cast(BooleanType())) \
    .withColumn("has_tv",
        when(col("attributes.HasTV").rlike("(?i)true"), True)
       .otherwise(False).cast(BooleanType())) \
    .withColumn("delivery",
        when(col("attributes.RestaurantsDelivery").rlike("(?i)true"), True)
       .otherwise(False).cast(BooleanType())) \
    .withColumn("takeout",
        when(col("attributes.RestaurantsTakeOut").rlike("(?i)true"), True)
       .otherwise(False).cast(BooleanType())) \
    .withColumn("reservations",
        when(col("attributes.RestaurantsReservations").rlike("(?i)true"), True)
       .otherwise(False).cast(BooleanType())) \
    .withColumn("good_for_kids",
        when(col("attributes.GoodForKids").rlike("(?i)true"), True)
       .otherwise(False).cast(BooleanType())) \
    .withColumn("alcohol",
        when(col("attributes.Alcohol").rlike("full_bar"), "full_bar")
       .when(col("attributes.Alcohol").rlike("beer_and_wine"), "beer_and_wine")
       .otherwise("none")) \
    .withColumn("noise_level",
        regexp_extract(
            coalesce(col("attributes.NoiseLevel"), lit("")), r"'?(\w+)'?", 1)) \
    .withColumn("attire",
        regexp_extract(
            coalesce(col("attributes.RestaurantsAttire"), lit("")),
            r"'?(\w+)'?", 1)) \
    .drop("attributes")

# COMMAND ----------
# STEP 5 — Explode categories
# Keep categories_raw (comma-sep string) on main table.
# Write an exploded bridge table for category-grain analysis.
df_with_cats = df_attrs \
    .withColumn("categories_list",
        split(coalesce(col("categories"), lit("")), ",")) \
    .withColumn("category_count", size(col("categories_list")))

df_categories_exploded = df_with_cats.select(
    col("business_id"),
    trim(explode(col("categories_list"))).alias("category")
).filter(col("category") != "")

(df_categories_exploded
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema","true")
    .save(SILVER_CATS))
print(f"Business categories bridge: {df_categories_exploded.count():,} rows")

# COMMAND ----------
# STEP 6 — Validate and quarantine
df_tagged = df_with_cats.withColumn(
    "reject_reason",
    when(col("business_id").isNull(),     lit("null_business_id"))
   .when(col("business_name").isNull(),   lit("null_name"))
   .when(col("latitude").isNull()
       | col("longitude").isNull(),       lit("null_coordinates"))
   .when((col("stars") < 1) | (col("stars") > 5), lit("invalid_stars"))
   .otherwise(lit(None).cast("string"))
)

df_clean    = df_tagged.filter(col("reject_reason").isNull()).drop("reject_reason")
df_rejected = df_tagged.filter(col("reject_reason").isNotNull())

(df_rejected.write.format("delta").mode("append")
    .option("overwriteSchema","true").save(QUARANTINE))
print(f"Quarantined: {df_rejected.count():,}")

# COMMAND ----------
# STEP 7 — Add SCD Type 2 metadata columns
# change_hash fingerprints the tracked columns.
# If the hash changes between runs, PySpark MERGE expires the old
# row (sets valid_to = now, is_current = false) and inserts the new.
SCD_TRACK_COLS = [
    "stars", "review_count", "is_open",
    "city", "state", "categories", "price_range"
]

df_silver = df_clean \
    .withColumn("valid_from",  current_timestamp()) \
    .withColumn("valid_to",
        lit("9999-12-31 00:00:00").cast("timestamp")) \
    .withColumn("is_current",  lit(True)) \
    .withColumn("change_hash",
        sha2(concat_ws("|",
             *[col(c).cast("string") for c in SCD_TRACK_COLS]), 256)) \
    .withColumn("loaded_at",   current_timestamp())

# COMMAND ----------
# STEP 8 — MERGE into Silver Delta (SCD Type 2)
# Three cases handled:
#   NEW business   → insert as current
#   CHANGED values → expire old row, insert new current
#   UNCHANGED      → no-op
if DeltaTable.isDeltaTable(spark, SILVER) and MODE == "merge":
    silver_dt = DeltaTable.forPath(spark, SILVER)

    # Expire rows where hash changed
    silver_dt.alias("t").merge(
        df_silver.alias("s"),
        "t.business_id = s.business_id "
        "AND t.is_current = true "
        "AND t.change_hash != s.change_hash"
    ).whenMatchedUpdate(set={
        "valid_to":   "s.valid_from",
        "is_current": "false"
    }).execute()

    # Insert new/changed rows (won't match existing current+same hash)
    silver_dt.alias("t").merge(
        df_silver.alias("s"),
        "t.business_id = s.business_id "
        "AND t.change_hash = s.change_hash "
        "AND t.is_current = true"
    ).whenNotMatchedInsertAll().execute()

else:
    # Full rebuild or first run
    (df_silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema","true")
        .partitionBy("state")
        .save(SILVER))

current_count = (spark.read.format("delta").load(SILVER)
                     .filter("is_current = true").count())
print(f"Silver business MERGE complete. Current rows: {current_count:,}")

# COMMAND ----------
# STEP 9 — OPTIMIZE + ZORDER
spark.sql(f"OPTIMIZE delta.`{SILVER}` ZORDER BY (city, stars)")
print("OPTIMIZE complete.")
