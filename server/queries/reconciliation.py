"""Reconciliation queries for validating cost accuracy.

These queries cross-check totals across different aggregation dimensions
to detect double-counting, missing attribution, or data quality issues.
"""

# =============================================================================
# CHECK 1: Total spend reconciliation
# Compare raw billing total vs what the summary endpoint reports
# =============================================================================
RECON_GROUND_TRUTH = """
SELECT
  SUM(u.usage_quantity) as total_dbus,
  SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  COUNT(*) as total_rows,
  COUNT(DISTINCT u.workspace_id) as workspace_count,
  COUNT(DISTINCT u.usage_date) as days_in_range
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date BETWEEN :start_date AND :end_date
  AND u.usage_quantity > 0
"""

# =============================================================================
# CHECK 2: Product category completeness
# Sum spend by product category — must equal ground truth total
# Uses the FAST query logic (no query.history join) for a clean comparison
# =============================================================================
RECON_BY_PRODUCT = """
WITH usage_with_price AS (
  SELECT
    u.usage_quantity,
    COALESCE(p.pricing.default, 0) as price_per_dbu,
    CASE
      WHEN u.billing_origin_product = 'SQL' THEN 'SQL'
      WHEN u.billing_origin_product = 'DLT' OR u.usage_metadata.dlt_pipeline_id IS NOT NULL THEN 'ETL - Streaming'
      WHEN u.billing_origin_product = 'JOBS' THEN 'ETL - Batch'
      WHEN u.sku_name LIKE '%ALL_PURPOSE%' THEN 'Interactive'
      WHEN u.sku_name LIKE '%SERVERLESS%' AND u.billing_origin_product NOT IN ('JOBS', 'SQL', 'DLT') THEN 'Serverless'
      WHEN u.sku_name LIKE '%INFERENCE%' THEN 'Model Serving'
      ELSE 'Other'
    END as product_category
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
    AND u.cloud = p.cloud
    AND p.price_end_time IS NULL
  WHERE u.usage_date BETWEEN :start_date AND :end_date
    AND u.usage_quantity > 0
)
SELECT
  product_category,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * price_per_dbu) as total_spend,
  COUNT(*) as row_count
FROM usage_with_price
GROUP BY product_category
ORDER BY total_spend DESC
"""

# =============================================================================
# CHECK 3: Workspace completeness
# Sum spend by workspace — must equal ground truth total
# =============================================================================
RECON_BY_WORKSPACE = """
SELECT
  u.workspace_id,
  SUM(u.usage_quantity) as total_dbus,
  SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  COUNT(*) as row_count
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date BETWEEN :start_date AND :end_date
  AND u.usage_quantity > 0
GROUP BY u.workspace_id
ORDER BY total_spend DESC
"""

# =============================================================================
# CHECK 4: SQL attribution accuracy
# Total SQL spend from billing.usage vs attributed Genie+DBSQL spend
# If attributed > actual, we have double-counting
# =============================================================================
RECON_SQL_ATTRIBUTION = """
WITH sql_billing_total AS (
  SELECT
    SUM(u.usage_quantity) as billing_dbus,
    SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) as billing_spend
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
    AND u.cloud = p.cloud
    AND p.price_end_time IS NULL
  WHERE u.billing_origin_product = 'SQL'
    AND u.usage_date BETWEEN :start_date AND :end_date
    AND u.usage_quantity > 0
),
sql_query_work AS (
  SELECT
    CASE
      WHEN client_application LIKE '%Genie%' THEN 'SQL - Genie'
      ELSE 'SQL - DBSQL'
    END AS product_category,
    DATE(start_time) AS usage_date,
    compute.warehouse_id AS warehouse_id,
    SUM(total_task_duration_ms) AS work_ms
  FROM system.query.history
  WHERE executed_as_user_id IS NOT NULL
    AND compute.warehouse_id IS NOT NULL
    AND start_time >= CAST(:start_date AS TIMESTAMP)
    AND start_time < CAST(DATE_ADD(CAST(:end_date AS DATE), 1) AS TIMESTAMP)
  GROUP BY 1, 2, 3
),
sql_usage AS (
  SELECT
    u.usage_date,
    u.usage_metadata.warehouse_id as warehouse_id,
    SUM(u.usage_quantity) as total_dbus,
    SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
    AND u.cloud = p.cloud
    AND p.price_end_time IS NULL
  WHERE u.billing_origin_product = 'SQL'
    AND u.usage_date BETWEEN :start_date AND :end_date
    AND u.usage_quantity > 0
  GROUP BY 1, 2
),
warehouse_totals AS (
  SELECT
    usage_date,
    warehouse_id,
    SUM(work_ms) as total_work_ms
  FROM sql_query_work
  GROUP BY usage_date, warehouse_id
),
sql_attributed AS (
  SELECT
    q.product_category,
    CASE
      WHEN w.total_work_ms > 0 THEN (q.work_ms / w.total_work_ms) * s.total_dbus
      ELSE 0
    END as attributed_dbus,
    CASE
      WHEN w.total_work_ms > 0 THEN (q.work_ms / w.total_work_ms) * s.total_spend
      ELSE 0
    END as attributed_spend
  FROM sql_query_work q
  JOIN warehouse_totals w ON q.usage_date = w.usage_date AND q.warehouse_id = w.warehouse_id
  LEFT JOIN sql_usage s ON q.usage_date = s.usage_date AND q.warehouse_id = s.warehouse_id
),
attributed_totals AS (
  SELECT
    SUM(attributed_dbus) as attributed_dbus,
    SUM(attributed_spend) as attributed_spend
  FROM sql_attributed
)
SELECT
  b.billing_dbus,
  b.billing_spend,
  a.attributed_dbus,
  a.attributed_spend,
  a.attributed_spend - b.billing_spend as spend_difference,
  CASE
    WHEN b.billing_spend > 0 THEN ROUND(100.0 * (a.attributed_spend - b.billing_spend) / b.billing_spend, 4)
    ELSE 0
  END as spend_difference_pct
FROM sql_billing_total b
CROSS JOIN attributed_totals a
"""

# =============================================================================
# CHECK 5: Duplicate detection in system.query.history
# If COUNT(*) >> COUNT(DISTINCT statement_id), duplicates exist
# =============================================================================
RECON_QUERY_HISTORY_DUPES = """
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT statement_id) as unique_statements,
  COUNT(*) - COUNT(DISTINCT statement_id) as duplicate_rows,
  CASE
    WHEN COUNT(DISTINCT statement_id) > 0
    THEN ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT statement_id)) / COUNT(*), 4)
    ELSE 0
  END as duplicate_pct
FROM system.query.history
WHERE start_time >= CAST(:start_date AS TIMESTAMP)
  AND start_time < CAST(DATE_ADD(CAST(:end_date AS DATE), 1) AS TIMESTAMP)
"""

# =============================================================================
# CHECK 6: List prices join quality
# Check how many billing rows get a NULL price (no matching list_prices row)
# =============================================================================
RECON_PRICE_COVERAGE = """
SELECT
  COUNT(*) as total_rows,
  COUNT(CASE WHEN p.pricing.default IS NOT NULL THEN 1 END) as priced_rows,
  COUNT(CASE WHEN p.pricing.default IS NULL THEN 1 END) as unpriced_rows,
  ROUND(100.0 * COUNT(CASE WHEN p.pricing.default IS NULL THEN 1 END) / COUNT(*), 4) as unpriced_pct,
  SUM(u.usage_quantity) as total_dbus,
  SUM(CASE WHEN p.pricing.default IS NULL THEN u.usage_quantity ELSE 0 END) as unpriced_dbus,
  ROUND(100.0 * SUM(CASE WHEN p.pricing.default IS NULL THEN u.usage_quantity ELSE 0 END) / SUM(u.usage_quantity), 4) as unpriced_dbu_pct
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date BETWEEN :start_date AND :end_date
  AND u.usage_quantity > 0
"""

# =============================================================================
# CHECK 7: NULL attribution audit
# What % of spend has no cluster/warehouse/job/pipeline attribution?
# =============================================================================
RECON_NULL_ATTRIBUTION = """
SELECT
  COUNT(*) as total_rows,
  SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  COUNT(CASE
    WHEN u.usage_metadata.cluster_id IS NULL
     AND u.usage_metadata.warehouse_id IS NULL
     AND u.usage_metadata.job_id IS NULL
     AND u.usage_metadata.dlt_pipeline_id IS NULL
     AND u.usage_metadata.endpoint_name IS NULL
    THEN 1 END) as fully_unattributed_rows,
  SUM(CASE
    WHEN u.usage_metadata.cluster_id IS NULL
     AND u.usage_metadata.warehouse_id IS NULL
     AND u.usage_metadata.job_id IS NULL
     AND u.usage_metadata.dlt_pipeline_id IS NULL
     AND u.usage_metadata.endpoint_name IS NULL
    THEN u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END) as unattributed_spend,
  ROUND(100.0 * SUM(CASE
    WHEN u.usage_metadata.cluster_id IS NULL
     AND u.usage_metadata.warehouse_id IS NULL
     AND u.usage_metadata.job_id IS NULL
     AND u.usage_metadata.dlt_pipeline_id IS NULL
     AND u.usage_metadata.endpoint_name IS NULL
    THEN u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END)
    / NULLIF(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 0), 4) as unattributed_pct
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date BETWEEN :start_date AND :end_date
  AND u.usage_quantity > 0
"""

# =============================================================================
# CHECK 8: Materialized view vs live query comparison
# Compare MV daily totals against live query totals
# =============================================================================
RECON_MV_VS_LIVE = """
SELECT
  usage_date,
  SUM(u.usage_quantity) as live_dbus,
  SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) as live_spend
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date BETWEEN :start_date AND :end_date
  AND u.usage_quantity > 0
GROUP BY usage_date
ORDER BY usage_date
"""

RECON_MV_DAILY_SUMMARY = """
SELECT
  usage_date,
  total_dbus as mv_dbus,
  total_spend as mv_spend
FROM {catalog}.{schema}.daily_usage_summary
WHERE usage_date BETWEEN :start_date AND :end_date
ORDER BY usage_date
"""

# =============================================================================
# CHECK 9: List prices uniqueness
# Verify each SKU+cloud has exactly one active price
# =============================================================================
RECON_PRICE_UNIQUENESS = """
SELECT
  sku_name,
  cloud,
  COUNT(*) as active_price_count,
  MIN(pricing.default) as min_price,
  MAX(pricing.default) as max_price
FROM system.billing.list_prices
WHERE price_end_time IS NULL
GROUP BY sku_name, cloud
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 20
"""
