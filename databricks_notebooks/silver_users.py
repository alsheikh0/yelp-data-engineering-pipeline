# Databricks notebook source
# Title: silver_users
# ─────────────────────────────────────────────────────────────────────────────
# user.json → silver_users (Delta, SCD Type 2)
#
# Key DE challenges:
#   1. `friends` — could be thousands of user_ids per user (social graph)
#      We store count only; write a separate bridge table for graph queries
#   2. `elite`   — array of ints (years with elite status)
#      Explode into a bridge table for "who was elite in 2019?" queries
#   3. SCD Type 2 — user_tier, review_count, and average_stars change
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
dbutils.widgets.text("storage_account", "yelpstorage", "Storage Account")
dbutils.widgets.text("snapshot_year",   "2022",        "Dataset Snapshot Year")
dbutils.widgets.dropdown("mode", "merge", ["merge", "full"], "Run Mode")

STORAGE       = dbutils.widgets.get("storage_account")
SNAPSHOT_YEAR = int(dbutils.widgets.get("snapshot_year"))
MODE          = dbutils.widgets.get("mode")

BRONZE        = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/yelp/user.json"
SILVER        = f"abfss://silver@{STORAGE}.dfs.core.windows.net/users/"
SILVER_ELITE  = f"abfss://silver@{STORAGE}.dfs.core.windows.net/user_elite_years/"
QUARANTINE    = f"abfss://silver@{STORAGE}.dfs.core.windows.net/rejected/users/"

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, size, when, lit, sha2, concat_ws, current_timestamp,
    to_date, explode, year, coalesce, array_contains, trim
)
from pyspark.sql.types import IntegerType, BooleanType
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName(f"silver_users_{MODE}") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------
# STEP 1 — Read raw JSON
df_raw = spark.read.json(BRONZE)
print(f"Raw users: {df_raw.count():,}")

# COMMAND ----------
# STEP 2 — Core fields + aggregated social signals
# friends field is ArrayType(StringType) — can have thousands of entries.
# Storing the full list per user is expensive and not analytically useful
# at the fact/dimension level. We store count and write a bridge table.
df_core = df_raw.select(
    col("user_id"),
    trim(col("name")).alias("user_name"),
    to_date(col("yelping_since"), "yyyy-MM-dd").alias("member_since"),
    col("review_count").cast(IntegerType()),
    col("average_stars").cast("double"),
    col("fans").cast(IntegerType()),
    coalesce(col("useful"), lit(0)).cast(IntegerType()).alias("votes_useful_given"),
    coalesce(col("funny"),  lit(0)).cast(IntegerType()).alias("votes_funny_given"),
    coalesce(col("cool"),   lit(0)).cast(IntegerType()).alias("votes_cool_given"),
    # Friend count — not the array itself
    size(coalesce(col("friends"), lit([]))).alias("friend_count"),
    # Elite array kept for bridge table extraction
    col("elite"),
    # Total compliments received — sum all compliment columns
    (coalesce(col("compliment_hot"),     lit(0)) +
     coalesce(col("compliment_more"),    lit(0)) +
     coalesce(col("compliment_profile"), lit(0)) +
     coalesce(col("compliment_cute"),    lit(0)) +
     coalesce(col("compliment_list"),    lit(0)) +
     coalesce(col("compliment_note"),    lit(0)) +
     coalesce(col("compliment_plain"),   lit(0)) +
     coalesce(col("compliment_cool"),    lit(0)) +
     coalesce(col("compliment_funny"),   lit(0)) +
     coalesce(col("compliment_writer"),  lit(0)) +
     coalesce(col("compliment_photos"),  lit(0))
    ).cast(IntegerType()).alias("total_compliments"),
)

# COMMAND ----------
# STEP 3 — Write elite years bridge table (before dropping elite column)
# This is the correct way to handle the elite array:
# one row per (user_id, year) so analysts can filter to any year's elite set.
df_elite_bridge = df_raw.select(
    col("user_id"),
    explode(col("elite").cast("array<int>")).alias("elite_year")
).filter(col("elite_year").isNotNull())

(df_elite_bridge.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true").save(SILVER_ELITE))
print(f"Elite years bridge: {df_elite_bridge.count():,} rows")

# COMMAND ----------
# STEP 4 — Derive SCD-tracked user tier + metadata
df_tiered = df_core \
    .withColumn("is_currently_elite",
        array_contains(
            col("elite").cast("array<int>"), SNAPSHOT_YEAR
        ).cast(BooleanType())) \
    .withColumn("total_elite_years",
        size(coalesce(col("elite").cast("array<int>"), lit([])))) \
    .withColumn("years_on_platform",
        (lit(SNAPSHOT_YEAR) - year(col("member_since"))).cast(IntegerType())) \
    .withColumn("user_tier",
        when(col("is_currently_elite"),              "elite")
       .when((col("review_count") >= 100)
           & (col("fans") >= 50),                   "veteran")
       .when(col("review_count") >= 25,              "active")
       .otherwise("newcomer")) \
    .withColumn("engagement_score",
        (col("review_count") * 3
         + col("fans") * 2
         + col("friend_count")
         + col("total_compliments")
        ).cast(IntegerType())) \
    .drop("elite")

# COMMAND ----------
# STEP 5 — Validate and quarantine
df_tagged = df_tiered.withColumn(
    "reject_reason",
    when(col("user_id").isNull(),     lit("null_user_id"))
   .when(col("user_name").isNull(),   lit("null_name"))
   .when(col("member_since").isNull(), lit("null_member_since"))
   .otherwise(lit(None).cast("string"))
)

df_clean    = df_tagged.filter(col("reject_reason").isNull()).drop("reject_reason")
df_rejected = df_tagged.filter(col("reject_reason").isNotNull())

(df_rejected.write.format("delta").mode("append")
    .option("overwriteSchema","true").save(QUARANTINE))
print(f"Quarantine users: {df_rejected.count():,}")

# COMMAND ----------
# STEP 6 — Add SCD Type 2 metadata
SCD_COLS = ["user_tier", "review_count", "fans", "average_stars", "is_currently_elite"]

df_silver = df_clean \
    .withColumn("valid_from",  current_timestamp()) \
    .withColumn("valid_to",    lit("9999-12-31 00:00:00").cast("timestamp")) \
    .withColumn("is_current",  lit(True)) \
    .withColumn("change_hash",
        sha2(concat_ws("|",
             *[col(c).cast("string") for c in SCD_COLS]), 256)) \
    .withColumn("loaded_at",   current_timestamp())

# COMMAND ----------
# STEP 7 — MERGE into Silver Delta (SCD Type 2)
if DeltaTable.isDeltaTable(spark, SILVER) and MODE == "merge":
    silver_dt = DeltaTable.forPath(spark, SILVER)

    # Expire old rows where tracked columns changed
    silver_dt.alias("t").merge(
        df_silver.alias("s"),
        "t.user_id = s.user_id "
        "AND t.is_current = true "
        "AND t.change_hash != s.change_hash"
    ).whenMatchedUpdate(set={
        "valid_to":   "s.valid_from",
        "is_current": "false"
    }).execute()

    # Insert new and changed rows
    silver_dt.alias("t").merge(
        df_silver.alias("s"),
        "t.user_id = s.user_id "
        "AND t.change_hash = s.change_hash "
        "AND t.is_current = true"
    ).whenNotMatchedInsertAll().execute()

else:
    (df_silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema","true")
        .save(SILVER))

current_count = (spark.read.format("delta").load(SILVER)
                     .filter("is_current = true").count())
print(f"Silver users current rows: {current_count:,}")

# COMMAND ----------
spark.sql(f"OPTIMIZE delta.`{SILVER}` ZORDER BY (user_tier, review_count)")
print("OPTIMIZE complete.")
