SELECT
  CASE
    WHEN COUNT(*) >= CAST('{{job.parameters.min_daily_orders}}' AS BIGINT) THEN 1
    ELSE RAISE_ERROR('Daily order volume below threshold')
  END AS passed
FROM {{job.parameters.catalog}}.{{job.parameters.schema}}.silver_orders
WHERE order_date = DATE '{{job.parameters.run_date}}';
