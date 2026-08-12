.PHONY: help test test-contracts test-glue validate

help:
	@echo "test           Run every check (contracts + dbt glue)"
	@echo "test-contracts Cross-surface and structural checks for the skill's rules"
	@echo "test-glue      Regression tests for the generated PyDABs dbt glue"
	@echo "validate       Schema-validate the checked-in example bundles"

test: test-contracts test-glue

test-contracts:
	uv run --no-project --with pyyaml --with pytest pytest tests -q

# The glue suite runs inside the example bundle, against its pinned dbt-core and
# databricks-dbt-factory. Skipped with a notice when that venv is absent.
test-glue:
	@if [ -d examples/dbt-cosmos/orders_analytics_bundle/.venv ]; then \
		cd examples/dbt-cosmos/orders_analytics_bundle && uv run --no-project pytest tests -q; \
	else \
		echo "skipping test-glue: run 'make setup' in examples/dbt-cosmos/orders_analytics_bundle first"; \
	fi

validate:
	databricks bundle schema > /tmp/dabs-schema.json
	uv run --no-project --with check-jsonschema check-jsonschema \
		--schemafile /tmp/dabs-schema.json \
		examples/customer-orders/customer_orders_bundle/databricks.yml \
		examples/customer-orders/customer_orders_bundle/resources/*.yml \
		examples/dbt-cosmos/orders_analytics_bundle/resources/*.yml \
		examples/lakeflow-connect/orders_replication_bundle/databricks.yml \
		examples/lakeflow-connect/orders_replication_bundle/resources/*.yml
