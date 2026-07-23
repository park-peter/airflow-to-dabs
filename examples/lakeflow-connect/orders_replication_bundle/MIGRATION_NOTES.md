# Migration Notes

## Summary

Converted Airflow 3 DAG: `orders_replication`.

A recurring Snowflake→lakehouse replication (`SnowflakeSqlApiOperator`) plus a downstream transform
becomes a **two-resource** conversion: a **Lakeflow Connect ingestion pipeline** (Snowflake via a UC
foreign catalog) and a **Lakeflow job** whose `pipeline_task` triggers it, followed by the transform.

## Why Lakeflow Connect (not a notebook)

The source task is *recurring ingestion from a federatable source into Delta* — the classification-step
signal for Lakeflow Connect. Snowflake has **no dedicated managed connector**, so it ingests via a
**query-based foreign catalog** (`ingest_from_uc_foreign_catalog: true`), reading through a UC foreign
catalog federated from a Snowflake connection.

## Prerequisites (manual, before deploy)

| Item | Action |
|---|---|
| **UC connection to Snowflake** | Create out-of-band (`CREATE CONNECTION` / UI) — **not a bundle resource**. Set `--var snowflake_connection=<name>`. Auth via the connection's stored credentials; no inline secrets. |
| **Foreign catalog** | This bundle **creates** it (`resources/catalogs.yml`), which is why `databricks.yml` sets `engine: direct`. To reference an existing one instead, drop `catalogs.yml` and point `snowflake_foreign_catalog` at it. |
| **`options.database`** | Set `snowflake_database` to the Snowflake database the catalog exposes (required per CREATE FOREIGN CATALOG). |
| **Networking** | The workspace must be able to reach Snowflake (private link / firewall as applicable). |

## Behavior changes / decisions

- **Destination ownership**: Lakeflow Connect creates and owns `main.commerce.raw_orders`. If a table by
  that name already exists, ingestion fails — use a new landing table and a downstream merge/cutover.
- **Orchestration**: the ingestion pipeline is **triggered** (driven by the job's `pipeline_task`). A
  continuous/streaming connector would instead run standalone with a downstream `trigger.table_update`.
- **Connector release state**: verify the current release state of query-based foreign-catalog ingestion
  for Snowflake before relying on it in production.
- **Conversion type**: **rearchitecture**, not a 1:1 task swap — the Snowflake query became a managed
  ingestion pipeline landing a raw table, and the transform reads that table.

## Validation

```bash
cd orders_replication_bundle
./verify.sh                                             # offline: structural + schema checks
DATABRICKS_PROFILE=<profile> ./verify.sh                # + full `bundle validate --strict`
```

Full deploy additionally requires the UC connection to exist and Snowflake to be reachable.
