# Databricks notebook source
# Title: log_ingestion_run
# ─────────────────────────────────────────────────────────────────────────────
# Called by ADF after every ingestion run (via DatabricksNotebook activity).
# Reads row counts from each Bronze JSON file and writes a structured run log
# to a Delta metadata table in Silver.
#
# This gives you a queryable history of every pipeline run:
#   - How many rows did each source file have?
#   - Did the quarantine rate spike this run?
#   - What time did each notebook complete?
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------
dbutils.widgets.text("storage_account", "yelpstorage",   "Storage Account")
dbutils.widgets.text("pipeline_run_id", "manual_run",    "ADF Pipeline Run ID")

STORAGE        = dbutils.widgets.get("storage_account")
PIPELINE_RUN   = dbutils.widgets.get("pipeline_run_id")

BRONZE_BASE    = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/yelp"
SILVER_BASE    = f"abfss://silver@{STORAGE}.dfs.core.windows.net"
RUN_LOG_PATH   = f"abfss://silver@{STORAGE}.dfs.core.windows.net/pipeline_run_log/"

# COMMAND ----------
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime

spark = SparkSession.builder \
    .appName("log_ingestion_run") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------
# Count rows in each Bronze JSON file
# and each Silver Delta table (current rows only where applicable)
def count_bronze(name: str) -> int:
    try:
        return spark.read.json(f"{BRONZE_BASE}/{name}.json").count()
    except Exception as e:
        print(f"Could not count {name}: {e}")
        return -1

def count_silver(path: str, current_only: bool = False) -> int:
    try:
        df = spark.read.format("delta").load(f"{SILVER_BASE}/{path}/")
        if current_only:
            df = df.filter("is_current = true")
        return df.count()
    except Exception as e:
        print(f"Could not count silver/{path}: {e}")
        return -1

def count_quarantine(name: str) -> int:
    try:
        return spark.read.format("delta").load(
            f"{SILVER_BASE}/rejected/{name}/"
        ).count()
    except Exception:
        return 0

# COMMAND ----------
run_ts = datetime.utcnow().isoformat()

metrics = [
    ("business",  count_bronze("business"),  count_silver("business", current_only=True),  count_quarantine("business")),
    ("reviews",   count_bronze("review"),    count_silver("reviews"),                       count_quarantine("reviews")),
    ("users",     count_bronze("user"),      count_silver("users", current_only=True),      count_quarantine("users")),
    ("checkins",  count_bronze("checkin"),   count_silver("checkins"),                      count_quarantine("checkins")),
    ("tips",      count_bronze("tip"),       count_silver("tips"),                          count_quarantine("tips")),
    ("photos",    count_bronze("photo"),     count_silver("photos"),                        count_quarantine("photos")),
]

rows = [
    Row(
        pipeline_run_id  = PIPELINE_RUN,
        source_name      = source,
        bronze_row_count = bronze,
        silver_row_count = silver,
        quarantine_count = quarantine,
        quarantine_rate_pct = round(quarantine * 100.0 / max(bronze, 1), 2),
        run_timestamp    = run_ts,
        logged_at        = datetime.utcnow().isoformat(),
    )
    for source, bronze, silver, quarantine in metrics
]

df_log = spark.createDataFrame(rows)

(df_log.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(RUN_LOG_PATH))

print(f"Run log written for pipeline_run_id: {PIPELINE_RUN}")
df_log.show(truncate=False)

# COMMAND ----------
# Alert if quarantine rate > 5% for any source
high_quarantine = [(r.source_name, r.quarantine_rate_pct)
                   for r in rows if r.quarantine_rate_pct > 5.0]

if high_quarantine:
    alert_msg = "\n".join(
        [f"  {src}: {rate:.1f}% quarantine rate" for src, rate in high_quarantine]
    )
    print(f"\n*** WARNING: High quarantine rate detected ***\n{alert_msg}")
    # In production: send Teams/Slack webhook or raise exception to fail ADF activity
    # raise Exception(f"Quarantine rate exceeded 5% threshold:\n{alert_msg}")
else:
    print("\nAll sources within quarantine rate threshold (<5%). Pipeline healthy.")
