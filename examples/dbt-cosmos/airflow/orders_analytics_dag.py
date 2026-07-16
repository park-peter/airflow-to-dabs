"""Daily orders analytics: ingest raw orders, run the dbt project via cosmos, publish metrics.

The dbt project lives at /opt/airflow/dbt/orders_analytics on the Airflow host.
Cosmos renders one Airflow task per dbt model/seed/test at runtime from the dbt
manifest, giving per-model visibility and retries inside the Airflow UI.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles import DatabricksTokenProfileMapping

DBT_PROJECT_PATH = "/opt/airflow/dbt/orders_analytics"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["data-eng-alerts@example.com"],
    "email_on_failure": True,
}


def ingest_orders(**context):
    """Load the latest raw order files into the raw_orders table."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.json("s3://acme-orders/raw/")
    df.write.mode("append").saveAsTable("main.analytics.raw_orders")


def publish_metrics(**context):
    """Push the daily aggregates to the metrics store consumed by dashboards."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    daily = spark.read.table("main.analytics.fct_daily_orders")
    daily.write.mode("overwrite").saveAsTable("main.analytics.orders_dashboard_feed")


with DAG(
    dag_id="orders_analytics",
    description="Ingest raw orders, transform with dbt, publish daily metrics",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["orders", "dbt"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_orders",
        python_callable=ingest_orders,
    )

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="orders_analytics",
            target_name="dev",
            profile_mapping=DatabricksTokenProfileMapping(
                conn_id="databricks_default",
                profile_args={"catalog": "main", "schema": "analytics"},
            ),
        ),
        render_config=RenderConfig(test_behavior=TestBehavior.AFTER_EACH),
    )

    publish = PythonOperator(
        task_id="publish_metrics",
        python_callable=publish_metrics,
    )

    ingest >> dbt_transform >> publish
