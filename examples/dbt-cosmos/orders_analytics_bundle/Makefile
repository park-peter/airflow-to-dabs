# Bundle target for validate/deploy AND the dbt profile target for parsing.
# Per-target manifest paths (target/$(TARGET)/) keep artifacts parsed against one
# profile target (e.g. dev catalog/schema) out of another target's deployment.
TARGET ?= dev

.PHONY: setup manifest validate deploy run test

# Install the venv used by the PyDABs hook (databricks.yml -> python.venv_path).
# Uses your default package index; pass a private index if you need one, e.g.
#   make setup UV_INDEX_URL=<your-index-url>
setup:
	uv sync --dev

# Regenerate the dbt manifest read by the PyDABs hook in resources/.
# `dbt parse` does not connect to a warehouse; it only reads the project files.
# For multiple dbt-bearing DAGs sharing the project, one parse covers all.
manifest:
	uv run dbt deps --profiles-dir dbt_profiles
	uv run dbt parse --profiles-dir dbt_profiles --target $(TARGET) --target-path target/$(TARGET) --vars "$$(cat dbt_vars.json)"

validate: manifest
	databricks bundle validate -t $(TARGET)

deploy: manifest
	databricks bundle deploy -t $(TARGET)

# orders_analytics_job is the orchestrator (YAML) job key — running it triggers the
# generated dbt job through its run_job_task hop.
run:
	databricks bundle run orders_analytics_job -t $(TARGET)

# Regression tests for the PyDABs glue. Pure-logic tests need no dbt/warehouse;
# the end-to-end tests skip unless `make manifest` has produced target/dev/manifest.json.
test:
	uv run pytest tests
