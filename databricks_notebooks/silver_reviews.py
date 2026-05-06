# Databricks notebook source
# Title: silver_reviews
# ─────────────────────────────────────────────────────────────────────────────
# review.json → silver_reviews (Delta, ~7M rows)
# Partitioned by review_year / review_month for query performance.
#
# Key DE patterns shown here:
#   1. Cross-table FK validation against Silver business + Silver users
#   2. Quarantine with typed reject reasons
#   3. Text feature engineering (word count, char length, sentiment proxy)
#   4. Incremental append with deduplication guard
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
dbutils.widgets.text("storage_account", "yelpstorage", "Storage Account")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"], "Run Mode")

STORAGE = dbutils.widgets.get("storage_account")
MODE    = dbutils.widgets.get("mode")

BRONZE       = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/yelp/review.json"
SILVER       = f"abfss://silver@{STORAGE}.dfs.core.windows.net/reviews/"
SILVER_BIZ   = f"abfss://silver@{STORAGE}.dfs.core.windows.net/business/"
SILVER_USR   = f"abfss://silver@{STORAGE}.dfs.core.windows.net/users/"
QUARANTINE   = f"abfss://silver@{STORAGE}.dfs.core.windows.net/rejected/reviews/"

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, length, size, split,
    when, lit, trim, current_timestamp, coalesce,
    row_number, sha2, concat_ws
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName(f"silver_reviews_{MODE}") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------
# STEP 1 — Read raw JSON
df_raw = spark.read.json(BRONZE)
raw_count = df_raw.count()
print(f"Raw reviews: {raw_count:,}")

# COMMAND ----------
# STEP 2 — FK validation against Silver business and users
# We do LEFT joins and flag rows where the FK has no match.
# These are real orphan records in the dataset.
valid_biz = (
    spark.read.format("delta").load(SILVER_BIZ)
    .filter("is_current = true")
    .select(col("business_id").alias("_biz_id"))
    .distinct()
)
valid_usr = (
    spark.read.format("delta").load(SILVER_USR)
    .filter("is_current = true")
    .select(col("user_id").alias("_usr_id"))
    .distinct()
)

df_checked = (
    df_raw
    .join(valid_biz, df_raw.business_id == col("_biz_id"), "left")
    .join(valid_usr, df_raw.user_id    == col("_usr_id"),  "left")
    .withColumn("biz_valid", col("_biz_id").isNotNull())
    .withColumn("usr_valid", col("_usr_id").isNotNull())
    .drop("_biz_id", "_usr_id")
)

# COMMAND ----------
# STEP 3 — Tag reject reasons (single compound expression)
df_tagged = df_checked.withColumn(
    "reject_reason",
    when(col("review_id").isNull(),
         lit("null_review_id"))
   .when(~col("biz_valid"),
         lit("orphan_business_id"))
   .when(~col("usr_valid"),
         lit("orphan_user_id"))
   .when(col("stars").isNull()
       | (col("stars") < 1)
       | (col("stars") > 5),
         lit("invalid_stars"))
   .when(col("text").isNull()
       | (trim(col("text")) == ""),
         lit("empty_text"))
   .otherwise(lit(None).cast("string"))
)

df_clean    = df_tagged.filter(col("reject_reason").isNull())
df_rejected = df_tagged.filter(col("reject_reason").isNotNull())

(df_rejected.write.format("delta").mode("append")
    .option("overwriteSchema","true").save(QUARANTINE))
print(f"Quarantine: {df_rejected.count():,} rows")

# COMMAND ----------
# STEP 4 — Deduplicate on review_id
# Some users re-submit reviews; keep only the most recent version
dedup_win = Window.partitionBy("review_id").orderBy(col("date").desc())
df_deduped = (
    df_clean
    .withColumn("_rn", row_number().over(dedup_win))
    .filter(col("_rn") == 1)
    .drop("_rn")
)
dupes = df_clean.count() - df_deduped.count()
print(f"Duplicate reviews removed: {dupes:,}")

# COMMAND ----------
# STEP 5 — Parse, enrich, derive text features
df_silver = (
    df_deduped
    .withColumn("review_date",      to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("review_year",      year("review_date"))
    .withColumn("review_month",     month("review_date"))
    .withColumn("stars",            col("stars").cast(IntegerType()))
    .withColumn("useful",           coalesce(col("useful"), lit(0)).cast(IntegerType()))
    .withColumn("funny",            coalesce(col("funny"),  lit(0)).cast(IntegerType()))
    .withColumn("cool",             coalesce(col("cool"),   lit(0)).cast(IntegerType()))
    .withColumn("total_votes",      col("useful") + col("funny") + col("cool"))

    # Text metrics — computed without loading text into Gold
    .withColumn("text_char_len",    length(col("text")))
    .withColumn("text_word_count",  size(split(trim(col("text")), r"\s+")))

    # Sentiment proxy (stars-based; crude but queryable)
    .withColumn("sentiment_proxy",
        when(col("stars") >= 4, "positive")
       .when(col("stars") == 3, "neutral")
       .otherwise("negative"))

    # Review quality signal
    .withColumn("is_high_quality",
        ((col("text_word_count") >= 50) & (col("total_votes") >= 3))
        .cast("boolean"))

    .withColumn("loaded_at",        current_timestamp())

    # Drop raw text to save ~60% storage; Bronze is the replay source
    .drop("date", "text", "reject_reason", "biz_valid", "usr_valid")
)

# COMMAND ----------
# STEP 6 — Write Silver (incremental append or full overwrite)
if MODE == "incremental" and DeltaTable.isDeltaTable(spark, SILVER):
    # For incremental: MERGE on review_id to handle re-submitted reviews
    silver_dt = DeltaTable.forPath(spark, SILVER)
    (silver_dt.alias("t")
     .merge(df_silver.alias("s"), "t.review_id = s.review_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
else:
    (df_silver
     .write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema","true")
     .partitionBy("review_year", "review_month")
     .save(SILVER))

silver_count = spark.read.format("delta").load(SILVER).count()
print(f"Silver reviews total: {silver_count:,}")

# COMMAND ----------
# STEP 7 — OPTIMIZE + ZORDER on most common filter columns
spark.sql(f"OPTIMIZE delta.`{SILVER}` ZORDER BY (business_id, review_date)")
print("OPTIMIZE complete.")
