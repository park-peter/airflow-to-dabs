"""Airflow 3 DAG that incrementally replicates a Snowflake table into the
lakehouse on a schedule, then runs a downstream transform.

This is the shape that maps to Lakeflow Connect: a *recurring, incremental*
ingestion from a federatable source (Snowflake) into a Delta table, followed by a
transform. The source task reads Snowflake and **writes** the rows into a target
table using a cursor column (`updated_at`) and a primary key (`order_id`) — a real
transfer with an explicit destination, not a bare read. Snowflake has no dedicated
managed connector, so the migration ingests via a Unity Catalog *foreign catalog*
(query-based ingestion), carrying the same cursor + key into the pipeline's
`table_configuration`; the transform hangs off the ingestion pipeline via a
pipeline_task hop.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, get_current_context, task


BOOTSTRAP_WATERMARK = datetime(1970, 1, 1, tzinfo=timezone.utc)
SNOWFLAKE_CONN_ID = "snowflake_default"
DATABRICKS_CONN_ID = "databricks_default"
TARGET_TABLE = "main.commerce.raw_orders"


@task
def replicate_orders() -> None:
    """Upsert changed Snowflake orders into the Databricks Delta target."""
    context = get_current_context()
    watermark = context.get("prev_data_interval_end_success") or BOOTSTRAP_WATERMARK

    snowflake = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    rows = snowflake.get_records(
        "SELECT order_id, order_date, amount, updated_at "
        "FROM analytics.public.orders "
        "WHERE updated_at > %(watermark)s",
        parameters={"watermark": watermark},
    )

    databricks = DatabricksSqlHook(databricks_conn_id=DATABRICKS_CONN_ID)
    databricks.run(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
          order_id BIGINT,
          order_date DATE,
          amount DECIMAL(18, 2),
          updated_at TIMESTAMP
        ) USING DELTA
        """
    )
    if not rows:
        print(f"no orders changed after {watermark.isoformat()}")
        return

    values = []
    parameters = {}
    for index, (order_id, order_date, amount, updated_at) in enumerate(rows):
        values.append(
            f"(:order_id_{index}, :order_date_{index}, :amount_{index}, :updated_at_{index})"
        )
        parameters.update(
            {
                f"order_id_{index}": order_id,
                f"order_date_{index}": order_date,
                f"amount_{index}": amount,
                f"updated_at_{index}": updated_at,
            }
        )

    databricks.run(
        f"""
        MERGE INTO {TARGET_TABLE} AS target
        USING (
          SELECT * FROM VALUES {", ".join(values)}
          AS incoming(order_id, order_date, amount, updated_at)
        ) AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        parameters=parameters,
    )
    print(f"replicated {len(rows)} changed orders into raw_orders")


@task
def transform_orders(catalog: str, schema: str) -> None:
    print(f"building {catalog}.{schema}.gold_orders from replicated raw_orders")


@dag(
    dag_id="orders_replication",
    schedule="0 * * * *",
    catchup=False,
    tags=["orders", "airflow-to-dabs-demo", "lakeflow-connect"],
)
def orders_replication():
    replicate_orders() >> transform_orders(catalog="main", schema="commerce")


orders_replication()
