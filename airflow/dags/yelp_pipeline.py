"""
yelp_pipeline.py
────────────────────────────────────────────────────────────────────
Full Yelp DE pipeline DAG.

Flow:
  1. adf_ingest_yelp_files    → ADF copies 6 JSON files from source to ADLS Bronze
  2. wait_for_adf             → polls ADF run until terminal state
  3. silver_business          → Databricks job: business JSON → Silver (SCD MERGE)
  4. silver_users             → Databricks job: user JSON → Silver (SCD MERGE)
  5. silver_reviews           → Databricks job: review JSON → Silver (7M rows)
  6. silver_checkins          → Databricks job: checkin JSON → Silver (explode)
  7. silver_tips              → Databricks job: tip JSON → Silver
  8. dbt_staging              → dbt run --select staging
  9. dbt_gold_dims            → dbt run --select gold.dimensions
       (depends on silver_business + silver_users)
 10. dbt_gold_facts           → dbt run --select gold.facts
       (depends on dims)
 11. dbt_gold_aggs            → dbt run --select gold.aggregates
       (depends on facts)
 12. dbt_tests                → dbt test --select gold
 13. notify_success / notify_failure

Notes:
  - silver_business and silver_users run in PARALLEL (independent sources)
  - silver_reviews, silver_checkins, silver_tips run in PARALLEL
  - dim build waits for BOTH business and user Silver to complete
  - Max 1 active run — SCD MERGE is not safe to run concurrently
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.hooks.data_factory import AzureDataFactoryHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.trigger_rule import TriggerRule


DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            [Variable.get("alert_email", default_var="de-team@company.com")],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
}

ADF_CONN_ID       = "azure_data_factory_default"
DATABRICKS_CONN   = "databricks_default"
ADF_RG            = Variable.get("adf_resource_group",  default_var="rg-yelp")
ADF_FACTORY       = Variable.get("adf_factory_name",    default_var="adf-yelp")
ADF_PIPELINE      = Variable.get("adf_pipeline_name",   default_var="pl_ingest_yelp")
DBT_PROJECT_DIR   = Variable.get("dbt_project_dir",     default_var="/opt/airflow/dbt/yelp")
DBT_PROFILES_DIR  = Variable.get("dbt_profiles_dir",    default_var="/opt/airflow/dbt/profiles")
STORAGE_ACCOUNT   = Variable.get("storage_account",     default_var="yelpstorage")
SNAPSHOT_YEAR     = Variable.get("snapshot_year",       default_var="2022")

# Databricks Job IDs — set these in Airflow Variables
JOB_BUSINESS  = Variable.get("db_job_silver_business",  default_var="100001")
JOB_USERS     = Variable.get("db_job_silver_users",     default_var="100002")
JOB_REVIEWS   = Variable.get("db_job_silver_reviews",   default_var="100003")
JOB_CHECKINS  = Variable.get("db_job_silver_checkins",  default_var="100004")
JOB_TIPS      = Variable.get("db_job_silver_tips",      default_var="100005")
JOB_PHOTOS    = Variable.get("db_job_silver_photos",    default_var="100006")


def trigger_adf(**context) -> None:
    hook = AzureDataFactoryHook(azure_data_factory_conn_id=ADF_CONN_ID)
    run  = hook.run_pipeline(
        pipeline_name       = ADF_PIPELINE,
        resource_group_name = ADF_RG,
        factory_name        = ADF_FACTORY,
        parameters          = {"storage_account": STORAGE_ACCOUNT},
    )
    context["ti"].xcom_push(key="adf_run_id", value=run.run_id)
    print(f"ADF triggered — run_id: {run.run_id}")


def poll_adf(**context) -> None:
    run_id = context["ti"].xcom_pull(key="adf_run_id")
    hook   = AzureDataFactoryHook(azure_data_factory_conn_id=ADF_CONN_ID)
    status = hook.get_pipeline_run(
        run_id              = run_id,
        resource_group_name = ADF_RG,
        factory_name        = ADF_FACTORY,
    ).status
    print(f"ADF run {run_id} status: {status}")
    if status == "Succeeded":
        return
    if status in ("Failed", "Cancelled"):
        raise RuntimeError(f"ADF pipeline {run_id} ended with: {status}")
    raise RuntimeError(f"ADF still in progress ({status}) — retrying")


def run_dbt(command: str, select: str = "", **_) -> None:
    import subprocess
    cmd = (
        f"dbt {command} "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR} "
        f"--target prod "
        f"{'--select ' + select if select else ''}"
    )
    print(f"Running: {cmd}")
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(r.stdout)
    if r.returncode != 0:
        print(r.stderr)
        raise RuntimeError(f"dbt failed:\n{r.stderr}")


# ── Shared Databricks params ──────────────────────────────────────────────────
DB_COMMON = {"storage_account": STORAGE_ACCOUNT, "mode": "merge"}
DB_USR_PARAMS = {**DB_COMMON, "snapshot_year": SNAPSHOT_YEAR}


with DAG(
    dag_id            = "yelp_de_pipeline",
    description       = "Yelp — Bronze JSON → Silver (PySpark+SCD2) → Gold (dbt)",
    default_args      = DEFAULT_ARGS,
    start_date        = datetime(2024, 1, 1),
    schedule_interval = "0 3 * * 1",   # every Monday 03:00 UTC (weekly refresh)
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["yelp", "medallion", "scd2", "weekly"],
    doc_md            = __doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── 1. ADF ingest all 6 JSON files to Bronze ──────────────────────────────
    trigger_adf_task = PythonOperator(
        task_id         = "adf_ingest_yelp_files",
        python_callable = trigger_adf,
    )

    poll_adf_task = PythonOperator(
        task_id         = "wait_for_adf",
        python_callable = poll_adf,
        retries         = 60,
        retry_delay     = timedelta(minutes=5),
    )

    # ── 2. Silver notebooks — business + users run in parallel ────────────────
    silver_business = DatabricksRunNowOperator(
        task_id            = "silver_business",
        databricks_conn_id = DATABRICKS_CONN,
        job_id             = JOB_BUSINESS,
        notebook_params    = DB_COMMON,
        polling_period_seconds = 30,
    )

    silver_users = DatabricksRunNowOperator(
        task_id            = "silver_users",
        databricks_conn_id = DATABRICKS_CONN,
        job_id             = JOB_USERS,
        notebook_params    = DB_USR_PARAMS,
        polling_period_seconds = 30,
    )

    # ── 3. Silver — reviews, checkins, tips run in parallel ───────────────────
    # (These do not depend on business/users Silver — they run concurrently)
    silver_reviews = DatabricksRunNowOperator(
        task_id            = "silver_reviews",
        databricks_conn_id = DATABRICKS_CONN,
        job_id             = JOB_REVIEWS,
        notebook_params    = {**DB_COMMON, "mode": "incremental"},
        polling_period_seconds = 60,
    )

    silver_checkins = DatabricksRunNowOperator(
        task_id            = "silver_checkins",
        databricks_conn_id = DATABRICKS_CONN,
        job_id             = JOB_CHECKINS,
        notebook_params    = {"storage_account": STORAGE_ACCOUNT},
        polling_period_seconds = 30,
    )

    silver_tips = DatabricksRunNowOperator(
        task_id            = "silver_tips",
        databricks_conn_id = DATABRICKS_CONN,
        job_id             = JOB_TIPS,
        notebook_params    = {"storage_account": STORAGE_ACCOUNT},
        polling_period_seconds = 30,
    )

    silver_photos = DatabricksRunNowOperator(
        task_id            = "silver_photos",
        databricks_conn_id = DATABRICKS_CONN,
        job_id             = JOB_PHOTOS,
        notebook_params    = {"storage_account": STORAGE_ACCOUNT},
        polling_period_seconds = 30,
    )

    # Wait for all Silver to complete before dbt
    silver_done = EmptyOperator(
        task_id      = "silver_done",
        trigger_rule = TriggerRule.ALL_SUCCESS,
    )

    # ── 4. dbt build chain ────────────────────────────────────────────────────
    dbt_staging = PythonOperator(
        task_id         = "dbt_staging",
        python_callable = run_dbt,
        op_kwargs       = {"command": "run", "select": "staging"},
    )

    dbt_dims = PythonOperator(
        task_id         = "dbt_gold_dims",
        python_callable = run_dbt,
        op_kwargs       = {"command": "run", "select": "gold.dimensions"},
    )

    dbt_facts = PythonOperator(
        task_id         = "dbt_gold_facts",
        python_callable = run_dbt,
        op_kwargs       = {"command": "run", "select": "gold.facts"},
    )

    dbt_aggs = PythonOperator(
        task_id         = "dbt_gold_aggs",
        python_callable = run_dbt,
        op_kwargs       = {"command": "run", "select": "gold.aggregates"},
    )

    dbt_tests = PythonOperator(
        task_id         = "dbt_tests",
        python_callable = run_dbt,
        op_kwargs       = {"command": "test", "select": "gold"},
    )

    # ── 5. Notifications ──────────────────────────────────────────────────────
    notify_success = BashOperator(
        task_id      = "notify_success",
        bash_command = (
            'echo "Yelp pipeline succeeded" '
            '| mail -s "[SUCCESS] yelp_pipeline" '
            '"{{ var.value.alert_email }}" || true'
        ),
        trigger_rule = TriggerRule.ALL_SUCCESS,
    )

    notify_failure = BashOperator(
        task_id      = "notify_failure",
        bash_command = (
            'echo "Yelp pipeline FAILED — check Airflow logs" '
            '| mail -s "[FAILURE] yelp_pipeline" '
            '"{{ var.value.alert_email }}" || true'
        ),
        trigger_rule = TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(
        task_id      = "end",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    # ADF → all Silver notebooks in parallel
    start >> trigger_adf_task >> poll_adf_task
    poll_adf_task >> [silver_business, silver_users,
                      silver_reviews, silver_checkins, silver_tips, silver_photos]

    # All Silver → staging → dims → facts → aggs → tests
    [silver_business, silver_users,
     silver_reviews, silver_checkins, silver_tips, silver_photos] >> silver_done

    silver_done >> dbt_staging >> dbt_dims >> dbt_facts >> dbt_aggs >> dbt_tests
    dbt_tests >> [notify_success, notify_failure] >> end
