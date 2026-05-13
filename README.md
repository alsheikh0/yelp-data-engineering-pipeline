# 🚀 Yelp Data Engineering Pipeline
[![Python](https://img.shields.io/badge/Language-Python-blue)](https://github.com/alsheikh0/yelp-data-engineering-pipeline)
[![Spark](https://img.shields.io/badge/Framework-PySpark-orange)](https://github.com/alsheikh0/yelp-data-engineering-pipeline)
[![Databricks](https://img.shields.io/badge/Platform-Azure%20Databricks-red)](https://github.com/alsheikh0/yelp-data-engineering-pipeline)

## 📌 Overview
This project implements a production-grade **Medallion Architecture** pipeline on **Azure Databricks**, utilizing the [Yelp Open Dataset](https://www.yelp.com/dataset). The pipeline processes over 10 million rows across 6 JSON source files (8.65 GB) to deliver analytics-ready data.

### Key Features:
* **SCD Type 2 Implementation:** Using Delta `MERGE` and SHA-256 hashing for history tracking.
* **PySpark ETL:** Complex transformations including struct flattening, array exploding, and FK validation.
* **Gold Layer Modeling:** Powering analytics via **dbt** with point-in-time joins.
* **Orchestration:** Automated workflows managed by **Airflow**.

---

## 🏗️ Architecture
The pipeline follows the standard Bronze-Silver-Gold pattern to ensure data quality and lineage.

```mermaid
flowchart TD
    A[Bronze Layer: Raw JSON] --> B[Silver Layer: PySpark Transformations]
    B --> C[SCD Type 2 Dimensions Delta Lake]
    C --> D[Gold Layer: dbt Models]
    D --> E[Airflow DAG Orchestration]
