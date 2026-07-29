#!/usr/bin/env bash
# Verify the Lakeflow Connect example bundle.
#
# CLI is always required (the offline path uses the credential-free `bundle schema`).
# A workspace profile is optional: set DATABRICKS_PROFILE to run full workspace
# validation; without it, only the credential-free schema + structural checks run.
#
#   DATABRICKS_CLI=databricks DATABRICKS_PROFILE=my-profile ./verify.sh   # full
#   ./verify.sh                                                           # offline only
set -euo pipefail
cd "$(dirname "$0")"

DATABRICKS_CLI="${DATABRICKS_CLI:-databricks}"
command -v "$DATABRICKS_CLI" >/dev/null 2>&1 || {
  echo "ERROR: Databricks CLI not found. Set DATABRICKS_CLI to its path." >&2
  exit 1
}

echo "== Structural assertions (yq) =="
ING=resources/orders_ingestion.pipeline.yml
CAT=resources/catalogs.yml
JOB=resources/orders_replication_job.yml
PIPE=.resources.pipelines.orders_ingestion
TBL="$PIPE.ingestion_definition.objects[0].table"

# require(<file> <expr>): fail unless the expression yields a non-null, non-empty scalar.
# `yq -e` errors on null/false; the != "" guard also rejects an empty string.
require() { yq -e "$1 // \"\" | select(. != \"\")" "$2" >/dev/null 2>&1; }
# absent(<file> <expr>): fail unless the key is absent (has(...) == false).
absent() { [ "$(yq "$1" "$2")" = "false" ]; }

[ "$(yq "$PIPE.ingestion_definition.ingest_from_uc_foreign_catalog" "$ING")" = "true" ] \
  || { echo "FAIL: ingest_from_uc_foreign_catalog not true"; exit 1; }
absent "$PIPE.ingestion_definition | has(\"connection_name\")" "$ING" \
  || { echo "FAIL: foreign-catalog ingestion must not set connection_name"; exit 1; }
# gateway_definition is a PIPELINE-level sibling of ingestion_definition, not nested in it.
absent "$PIPE | has(\"gateway_definition\")" "$ING" \
  || { echo "FAIL: ingestion pipeline must not carry gateway_definition"; exit 1; }
for k in source_catalog source_schema source_table destination_catalog destination_schema; do
  require "$TBL.$k" "$ING" || { echo "FAIL: missing $k on ingestion object"; exit 1; }
done
# query-based (non-CDC) ingestion needs a cursor and (unless append-only) primary keys
require "$TBL.table_configuration.query_based_connector_config.cursor_columns[0]" "$ING" \
  || { echo "FAIL: query-based ingestion missing cursor_columns"; exit 1; }
require "$TBL.table_configuration.primary_keys[0]" "$ING" \
  || { echo "FAIL: query-based ingestion missing primary_keys"; exit 1; }
[ "$(yq "$TBL.destination_table" "$ING")" = "raw_orders" ] \
  || { echo "FAIL: destination_table must be raw_orders for the downstream transform"; exit 1; }
require ".resources.catalogs.snowflake_analytics.connection_name" "$CAT" \
  || { echo "FAIL: foreign catalog missing connection_name"; exit 1; }
require ".resources.catalogs.snowflake_analytics.options.database" "$CAT" \
  || { echo "FAIL: foreign catalog missing options.database"; exit 1; }
[ "$(yq '.bundle.engine' databricks.yml)" = "direct" ] \
  || { echo "FAIL: bundle.engine must be direct for catalogs"; exit 1; }
yq '.resources.jobs.orders_replication_job.tasks[] | select(.task_key=="replicate_orders") | .pipeline_task.pipeline_id' "$JOB" \
  | grep -q 'resources.pipelines.orders_ingestion.id' \
  || { echo "FAIL: pipeline_task does not reference the ingestion pipeline"; exit 1; }

SOURCE_DAG=../airflow/orders_replication_dag.py
OPERATOR_MAPPING=../../../references/operator-mapping.md
COPILOT_RULES=../../../copilot-instructions.md
grep -Fq 'get_current_context()' "$SOURCE_DAG" \
  || { echo "FAIL: source DAG must read the runtime context for its watermark"; exit 1; }
grep -Fq 'BOOTSTRAP_WATERMARK' "$SOURCE_DAG" \
  || { echo "FAIL: source DAG must define a first-run bootstrap watermark"; exit 1; }
grep -Fq 'DatabricksSqlHook(' "$SOURCE_DAG" \
  || { echo "FAIL: source DAG must implement its Databricks destination write"; exit 1; }
grep -Fq 'MERGE INTO {TARGET_TABLE}' "$SOURCE_DAG" \
  || { echo "FAIL: source DAG must implement its raw_orders upsert"; exit 1; }
! grep -Fq '{{ prev_data_interval_end_success }}' "$SOURCE_DAG" \
  || { echo "FAIL: Jinja inside a TaskFlow callable is not rendered"; exit 1; }
! grep -Fq '# ... upsert' "$SOURCE_DAG" \
  || { echo "FAIL: source DAG still contains a placeholder destination write"; exit 1; }
grep -Fq 'Provider-specific operators bind to their database hooks' "$OPERATOR_MAPPING" \
  || { echo "FAIL: operator mapping must distinguish provider-specific SQL operators"; exit 1; }
grep -Fq 'Provider-specific `PostgresOperator` / `MySqlOperator`' "$COPILOT_RULES" \
  || { echo "FAIL: Copilot SQL operator guidance is stale"; exit 1; }
grep -Fq 'foreign-catalog ingestion covers all Lakehouse Federation sources' "$COPILOT_RULES" \
  || { echo "FAIL: Copilot foreign-catalog source guidance is stale"; exit 1; }
! grep -Fq "can't combine with a normal pipeline's" "$COPILOT_RULES" \
  || { echo "FAIL: Copilot ingestion field-combination guidance is stale"; exit 1; }
echo "  structural checks OK"

echo "== Offline schema validation (uv + check-jsonschema, credential-free) =="
"$DATABRICKS_CLI" bundle schema > /tmp/lfc-bundle.schema.json
uv run --with check-jsonschema check-jsonschema \
  --schemafile /tmp/lfc-bundle.schema.json \
  databricks.yml resources/*.yml
echo "  schema validation OK"

if [ -n "${DATABRICKS_PROFILE:-}" ]; then
  echo "== Full workspace validation (profile: $DATABRICKS_PROFILE) =="
  "$DATABRICKS_CLI" bundle validate --strict --target dev --profile "$DATABRICKS_PROFILE"
else
  echo "== Skipping workspace validation (set DATABRICKS_PROFILE to enable) =="
fi

echo "All checks passed."
