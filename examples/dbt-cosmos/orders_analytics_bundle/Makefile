.PHONY: setup manifest validate deploy run

# Install the venv used by the PyDABs hook (databricks.yml -> python.venv_path).
setup:
	uv sync --dev

# Regenerate the dbt manifest read by resources/orders_analytics_dbt_job.py.
# `dbt parse` does not connect to a warehouse; it only reads the project files.
manifest:
	uv run dbt deps --profiles-dir dbt_profiles
	uv run dbt parse --profiles-dir dbt_profiles --target dev

validate: manifest
	databricks bundle validate -t dev

deploy: manifest
	databricks bundle deploy -t dev

run:
	databricks bundle run orders_analytics_job -t dev
