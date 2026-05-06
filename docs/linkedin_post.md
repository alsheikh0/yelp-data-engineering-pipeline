# LinkedIn Post — Yelp DE Project Announcement

## Short version (for Featured / project card)
Built a production-grade Medallion Architecture pipeline on Azure using the
Yelp Open Dataset (8.65 GB, 6 JSON source files, ~10M+ rows post-processing).
Showcases SCD Type 2 via Delta MERGE, complex PySpark transformations
(struct flattening, array exploding, cross-table FK validation), dbt Gold
with point-in-time joins, and Airflow orchestration.

---

## Full LinkedIn post

Just completed my second major Azure data engineering portfolio project — this
time using the Yelp Open Dataset.

Here's what makes this one technically demanding:

SOURCE COMPLEXITY:
6 separate JSON files simulating 6 different source systems. Each has different
schema, different grain, different quality issues. This is what real multi-source
pipelines look like.

BRONZE → SILVER (PySpark on Databricks):

business.json — The `attributes` field is a struct with 50+ keys, values that
are strings, Pythonic booleans ("u'free'"), and nested JSON strings. I flatten
the high-value scalar attributes by name, apply regex cleaning, and write an
exploded bridge table for category-grain analysis.

checkin.json — The `date` field is a single comma-separated string of ALL
timestamps a business ever had a checkin. I split, explode, and parse into
proper event-grain rows — with a pre/post count assertion to catch silent data
loss in the explode operation.

review.json — 7M rows. FK validation against Silver business and Silver users
before writing. Orphan rows (reviews for businesses not in business.json) are
quarantined with typed reason codes, not silently dropped.

user.json — `friends` is an array of thousands of user_ids per user. I store
count only on the main table and write a separate (user_id, friend_id) bridge
for graph queries. `elite` array exploded into a bridge table for year-grain
elite analysis.

SCD TYPE 2 on business + user dims using Delta MERGE:
→ Compute change_hash (SHA-256) on tracked columns
→ MERGE 1: expire old rows where hash changed
→ MERGE 2: insert new/changed rows
→ Result: full history of every business rating change, open/close transition,
   and user tier promotion — queryable at any point in time

SILVER → GOLD (dbt):
→ dim_business and dim_user with valid_from/valid_to SCD columns
→ fact_reviews: point-in-time SCD join — a 2018 review sees the 2018 version
  of a business, not the current one
→ agg_business_monthly: MoM review volume change, rolling 3M avg stars, city rank
→ agg_city_category: YoY growth by city × category with growth_label column
→ agg_user_cohort: retention analysis by join-year cohort, elite conversion rate

ORCHESTRATION: Airflow DAG with parallel Silver task groups (business + users
in parallel, reviews + checkins + tips in parallel), sequential dbt chain with
dependency enforcement, SCD overlap test and orphan FK test as quality gates.

Tech: Python · PySpark · Delta Lake · dbt-databricks · ADF · ADLS Gen2 ·
Databricks · Airflow

GitHub: [link]

#DataEngineering #Azure #PySpark #dbt #DeltaLake #Databricks #Airflow #SCD
