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

[ "$(yq '.resources.pipelines.orders_ingestion.ingestion_definition.ingest_from_uc_foreign_catalog' "$ING")" = "true" ] \
  || { echo "FAIL: ingest_from_uc_foreign_catalog not true"; exit 1; }
[ "$(yq '.resources.pipelines.orders_ingestion.ingestion_definition | has("connection_name")' "$ING")" = "false" ] \
  || { echo "FAIL: foreign-catalog ingestion must not set connection_name"; exit 1; }
[ "$(yq '.resources.pipelines.orders_ingestion.ingestion_definition | has("gateway_definition")' "$ING")" = "false" ] \
  || { echo "FAIL: ingestion pipeline must not carry gateway_definition"; exit 1; }
[ -n "$(yq '.resources.pipelines.orders_ingestion.ingestion_definition.objects[0].table.source_catalog' "$ING")" ] \
  || { echo "FAIL: missing source_catalog/schema/table"; exit 1; }
[ -n "$(yq '.resources.catalogs.snowflake_analytics.connection_name' "$CAT")" ] \
  || { echo "FAIL: foreign catalog missing connection_name"; exit 1; }
[ -n "$(yq '.resources.catalogs.snowflake_analytics.options.database' "$CAT")" ] \
  || { echo "FAIL: foreign catalog missing options.database"; exit 1; }
[ "$(yq '.bundle.engine' databricks.yml)" = "direct" ] \
  || { echo "FAIL: bundle.engine must be direct for catalogs"; exit 1; }
yq '.resources.jobs.orders_replication_job.tasks[] | select(.task_key=="replicate_orders") | .pipeline_task.pipeline_id' "$JOB" \
  | grep -q 'resources.pipelines.orders_ingestion.id' \
  || { echo "FAIL: pipeline_task does not reference the ingestion pipeline"; exit 1; }
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
