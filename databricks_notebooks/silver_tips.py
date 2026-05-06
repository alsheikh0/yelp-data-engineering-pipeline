# Databricks notebook source
# Title: silver_tips
# ─────────────────────────────────────────────────────────────────────────────
# tip.json → silver_tips (Delta, ~1.3M rows)
#
# Tips are shorter, less-structured versions of reviews.
# Schema: { business_id, user_id, text, date, compliment_count }
# Key work: FK validation, text metrics, dedup, compliment normalization
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
dbutils.widgets.text("storage_account", "yelpstorage", "Storage Account")

STORAGE    = dbutils.widgets.get("storage_account")
BRONZE     = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/yelp/tip.json"
SILVER     = f"abfss://silver@{STORAGE}.dfs.core.windows.net/tips/"
SILVER_BIZ = f"abfss://silver@{STORAGE}.dfs.core.windows.net/business/"
SILVER_USR = f"abfss://silver@{STORAGE}.dfs.core.windows.net/users/"
QUARANTINE = f"abfss://silver@{STORAGE}.dfs.core.windows.net/rejected/tips/"

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, length, size, split, trim,
    when, lit, coalesce, current_timestamp, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("silver_tips") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------
df_raw = spark.read.json(BRONZE)
print(f"Raw tips: {df_raw.count():,}")

# COMMAND ----------
# FK checks against Silver business + user (current versions only)
valid_biz = (spark.read.format("delta").load(SILVER_BIZ)
    .filter("is_current = true")
    .select(col("business_id").alias("_biz")).distinct())

valid_usr = (spark.read.format("delta").load(SILVER_USR)
    .filter("is_current = true")
    .select(col("user_id").alias("_usr")).distinct())

df_checked = (df_raw
    .join(valid_biz, df_raw.business_id == col("_biz"), "left")
    .join(valid_usr, df_raw.user_id     == col("_usr"), "left")
    .withColumn("biz_valid", col("_biz").isNotNull())
    .withColumn("usr_valid", col("_usr").isNotNull())
    .drop("_biz", "_usr"))

# COMMAND ----------
# Validate + quarantine
df_tagged = df_checked.withColumn(
    "reject_reason",
    when(col("business_id").isNull(), lit("null_business_id"))
   .when(col("user_id").isNull(),     lit("null_user_id"))
   .when(~col("biz_valid"),           lit("orphan_business_id"))
   .when(~col("usr_valid"),           lit("orphan_user_id"))
   .when(col("text").isNull()
       | (trim(col("text")) == ""),   lit("empty_text"))
   .otherwise(lit(None).cast("string"))
)

df_clean    = df_tagged.filter(col("reject_reason").isNull())
df_rejected = df_tagged.filter(col("reject_reason").isNotNull())

(df_rejected.write.format("delta").mode("append")
    .option("overwriteSchema","true").save(QUARANTINE))
print(f"Quarantine tips: {df_rejected.count():,}")

# COMMAND ----------
# Deduplicate on (business_id, user_id, date) — same user can't tip same biz twice same day
dedup_win = Window.partitionBy("business_id","user_id","date").orderBy(col("date").desc())
df_deduped = (df_clean
    .withColumn("_rn", row_number().over(dedup_win))
    .filter(col("_rn") == 1)
    .drop("_rn"))

# COMMAND ----------
# Enrich
df_silver = df_deduped \
    .withColumn("tip_date",         to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("tip_year",         year("tip_date")) \
    .withColumn("tip_month",        month("tip_date")) \
    .withColumn("compliment_count",
        coalesce(col("compliment_count"), lit(0)).cast(IntegerType())) \
    .withColumn("text_char_len",    length(col("text"))) \
    .withColumn("text_word_count",  size(split(trim(col("text")), r"\s+"))) \
    .withColumn("is_popular_tip",   (col("compliment_count") >= 5).cast("boolean")) \
    .withColumn("loaded_at",        current_timestamp()) \
    .drop("date", "text", "reject_reason", "biz_valid", "usr_valid")

(df_silver
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .partitionBy("tip_year","tip_month")
    .save(SILVER))

print(f"Silver tips written: {df_silver.count():,}")
