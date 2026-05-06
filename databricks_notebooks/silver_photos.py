# Databricks notebook source
# Title: silver_photos
# ─────────────────────────────────────────────────────────────────────────────
# photo.json → silver_photos (Delta)
#
# Schema: { photo_id, business_id, caption, label }
# Label values: "food" | "drink" | "menu" | "inside" | "outside"
#
# Simplest of the 6 source files — but still requires:
#   1. FK validation against Silver business
#   2. Caption null/empty handling
#   3. Label taxonomy standardisation (raw values have inconsistencies)
#   4. Quarantine for orphan business_ids
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
dbutils.widgets.text("storage_account", "yelpstorage", "Storage Account")

STORAGE    = dbutils.widgets.get("storage_account")
BRONZE     = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/yelp/photo.json"
SILVER     = f"abfss://silver@{STORAGE}.dfs.core.windows.net/photos/"
SILVER_BIZ = f"abfss://silver@{STORAGE}.dfs.core.windows.net/business/"
QUARANTINE = f"abfss://silver@{STORAGE}.dfs.core.windows.net/rejected/photos/"

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, when, lit, length,
    current_timestamp, coalesce
)
from pyspark.sql.types import BooleanType

spark = SparkSession.builder \
    .appName("silver_photos") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------
# STEP 1 — Read raw JSON
# Schema: photo_id STRING, business_id STRING, caption STRING, label STRING
df_raw = spark.read.json(BRONZE)
print(f"Raw photos: {df_raw.count():,}")

# COMMAND ----------
# STEP 2 — FK validation against Silver business (current versions)
valid_biz = (
    spark.read.format("delta").load(SILVER_BIZ)
    .filter("is_current = true")
    .select(col("business_id").alias("_biz"))
    .distinct()
)

df_checked = (
    df_raw
    .join(valid_biz, df_raw.business_id == col("_biz"), "left")
    .withColumn("biz_valid", col("_biz").isNotNull())
    .drop("_biz")
)

# COMMAND ----------
# STEP 3 — Standardise label taxonomy
# Raw values are lowercase but may have leading/trailing spaces or
# unexpected values. Map to a controlled vocabulary.
VALID_LABELS = ["food", "drink", "menu", "inside", "outside"]

df_labelled = df_checked.withColumn(
    "label_clean",
    when(trim(lower(col("label"))).isin(VALID_LABELS),
         trim(lower(col("label"))))
    .otherwise("unknown")
)

# COMMAND ----------
# STEP 4 — Validate and quarantine
df_tagged = df_labelled.withColumn(
    "reject_reason",
    when(col("photo_id").isNull(),     lit("null_photo_id"))
   .when(col("business_id").isNull(),  lit("null_business_id"))
   .when(~col("biz_valid"),            lit("orphan_business_id"))
   .otherwise(lit(None).cast("string"))
)

df_clean    = df_tagged.filter(col("reject_reason").isNull()).drop("reject_reason")
df_rejected = df_tagged.filter(col("reject_reason").isNotNull())

(df_rejected.write.format("delta").mode("append")
    .option("overwriteSchema", "true").save(QUARANTINE))
print(f"Quarantine photos: {df_rejected.count():,}")

# COMMAND ----------
# STEP 5 — Enrich
df_silver = df_clean \
    .withColumn("photo_id",       col("photo_id")) \
    .withColumn("business_id",    col("business_id")) \
    .withColumn("label",          col("label_clean")) \
    .withColumn("caption",        trim(coalesce(col("caption"), lit("")))) \
    .withColumn("has_caption",    (length(trim(col("caption"))) > 0).cast(BooleanType())) \
    .withColumn("caption_length", length(col("caption"))) \
    .withColumn("loaded_at",      current_timestamp()) \
    .drop("label_clean", "biz_valid")

# COMMAND ----------
# STEP 6 — Write Silver, partitioned by label for efficient
# queries like "show all food photos for this business"
(df_silver
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("label")
    .save(SILVER))

silver_count = spark.read.format("delta").load(SILVER).count()
print(f"Silver photos written: {silver_count:,}")
print(f"  by label:")
(spark.read.format("delta").load(SILVER)
    .groupBy("label").count()
    .orderBy("label").show())
