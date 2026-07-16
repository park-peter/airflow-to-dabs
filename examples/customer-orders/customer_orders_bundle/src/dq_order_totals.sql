SELECT
  CASE
    WHEN COUNT(*) >= CAST(:min_daily_orders AS BIGINT) THEN 1
    ELSE RAISE_ERROR('Daily order volume below threshold')
  END AS passed
FROM IDENTIFIER(:catalog || '.' || :schema || '.silver_orders')
WHERE order_date = CAST(:run_date AS DATE);
