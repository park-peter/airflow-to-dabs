# Lakeflow Connect Ingestion Demo

A runnable demonstration of the `airflow-to-dabs` skill converting a recurring **Snowflake→lakehouse
replication** DAG into a **Lakeflow Connect** managed ingestion pipeline plus a transform job.

## Source DAG

`airflow/orders_replication_dag.py` — an Airflow 3 DAG (`airflow.sdk` + snowflake provider) that
replicates a Snowflake table hourly, then transforms it.

## Generated Bundle

`orders_replication_bundle/`

```text
orders_replication_bundle/
  databricks.yml                         # engine: direct (defines a foreign catalog); cli-version floor
  resources/
    catalogs.yml                         # Snowflake foreign catalog (connection_name + options.database)
    orders_ingestion.pipeline.yml        # query-based foreign-catalog ingestion -> Delta
    orders_replication_job.yml           # job: pipeline_task hop + transform
  src/
    transform_orders.py
  MIGRATION_NOTES.md
  verify.sh                              # structural + offline schema checks (full validate with a profile)
```

## Pattern Mapping

| Airflow | Lakeflow/DABs output | Notes |
|---|---|---|
| `SnowflakeSqlApiOperator` (recurring replication) | Lakeflow Connect **foreign-catalog** ingestion pipeline | No Snowflake managed connector — federated via `ingest_from_uc_foreign_catalog`. |
| `@task transform_orders` | `notebook_task` reading the replicated `raw_orders` | Standard TaskFlow → notebook. |
| DAG dependency `replicate >> transform` | `pipeline_task` (triggered) → `notebook_task` | Triggered pipeline runs as a `pipeline_task`; continuous would use a downstream `trigger.table_update`. |

## Talking Points

- Snowflake ingestion on Databricks is a **foreign-catalog** Lakeflow Connect pipeline, not a dedicated
  connector — the same path works for Redshift/BigQuery/Synapse.
- The **UC connection** is a manual prerequisite (not a bundle resource); the **foreign catalog** IS a
  bundle resource but requires `bundle.engine: direct`.
- Lakeflow Connect **owns the destination table** — existing targets need a landing table + cutover.

## Local QA

```bash
cd orders_replication_bundle && ./verify.sh
```

"Schema-valid with documented prerequisites" — deploying/running needs a real UC connection + reachable
Snowflake source.
