Yelp Data Engineering Project 🚀
📌 Overview
This project implements a production-grade Medallion Architecture pipeline on Azure Databricks, using the Yelp Open Dataset (8.65 GB, 6 JSON source files, ~10M+ rows post-processing).

It demonstrates:

SCD Type 2 with Delta MERGE

Complex PySpark transformations (struct flattening, array exploding, FK validation)

dbt Gold layer with point-in-time joins

Airflow orchestration for end-to-end automation

📂 Dataset
Source: Yelp Open Dataset

Size: 8.65 GB

Files: 6 JSON files (business, user, review, checkin, tip, photo)

Rows: ~10M+ after processing

🏗️ Architecture
mermaid
flowchart TD
    A[Bronze Layer: Raw JSON] --> B[Silver Layer: PySpark Transformations]
    B --> C[SCD Type 2 Dimensions (Delta Lake)]
    C --> D[Gold Layer: dbt Models]
    D --> E[Airflow DAG Orchestration]
🔹 Bronze → Silver (PySpark on Databricks)
business.json: Flattened attributes struct, regex cleaning, exploded category bridge

checkin.json: Split/explode comma-separated timestamps, event-grain rows with count assertions

review.json: 7M rows, FK validation against Silver business/users, orphan rows quarantined with typed reason codes

user.json: Friends array → bridge table, elite array → year-grain bridge

🔹 SCD Type 2 (Delta Lake)
Change tracking via SHA-256 hash

MERGE logic: expire old rows + insert new/changed rows

Full history of business rating changes, open/close transitions, and user tier promotions

🔹 Silver → Gold (dbt)
dim_business / dim_user: Valid_from / valid_to SCD columns

fact_reviews: Point-in-time joins (reviews linked to historical business/user state)

agg_business_monthly: MoM review volume, rolling 3M avg stars, city rank

agg_city_category: YoY growth by city × category with growth labels

agg_user_cohort: Cohort retention, elite conversion rate

🔹 Orchestration (Airflow)
Parallel Silver task groups (business + users, reviews + checkins + tips)

Sequential dbt chain with dependency enforcement

Quality gates: SCD overlap test, orphan FK test

⚙️ Tech Stack
Languages: Python, PySpark

Data Lake: Delta Lake, ADLS Gen2

Transformation: Databricks, dbt-databricks

Orchestration: Airflow

Integration: Azure Data Factory

📈 Key Learnings
Handling multi-source JSON ingestion with schema drift and quality issues

Designing robust FK validation and quarantine strategies

Implementing SCD Type 2 for historical tracking

Building point-in-time joins for accurate analytics

Orchestrating pipelines with Airflow DAGs and quality gates

📁 Repository Structure
Code
├── bronze/          # Raw ingestion
├── silver/          # Cleaned & conformed tables
├── gold/            # dbt models & aggregates
├── airflow/         # DAGs for orchestration
├── notebooks/       # PySpark transformations
└── docs/            # Architecture diagrams & notes
