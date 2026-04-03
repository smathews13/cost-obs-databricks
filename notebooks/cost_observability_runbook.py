# Databricks notebook source
# MAGIC %md
# MAGIC # Cost Observability Runbook
# MAGIC
# MAGIC This notebook provides comprehensive cost analysis for Databricks workspaces and accounts. It should be run in your Databricks account using the run-all button above and should function correctly using serverless compute.
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - `system.billing.usage` - All usage records with DBU consumption
# MAGIC - `system.billing.list_prices` - Current pricing by SKU
# MAGIC - `system.compute.clusters` - Cluster configurations
# MAGIC - `system.query.history` - Query logs for Genie attribution
# MAGIC
# MAGIC **Key Metrics:**
# MAGIC - Total spend and DBU consumption
# MAGIC - Cost breakdown by product (SQL, Jobs, All Purpose, etc.)
# MAGIC - Workspace-level attribution
# MAGIC - Daily spending trends
# MAGIC - Top cost drivers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configure date range for analysis
from datetime import datetime, timedelta

# Default to last 30 days
end_date = datetime.now().date()
start_date = end_date - timedelta(days=30)

# Override with custom dates if needed
# start_date = "2025-01-01"
# end_date = "2025-01-31"

print(f"Analyzing costs from {start_date} to {end_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Account Summary
# MAGIC Total costs across all workspaces

# COMMAND ----------

# DBTITLE 1,Total Account Spend
summary_df = spark.sql(f"""
SELECT
  COUNT(DISTINCT workspace_id) as total_workspaces,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  MIN(usage_date) as first_date,
  MAX(usage_date) as last_date,
  DATEDIFF(MAX(usage_date), MIN(usage_date)) + 1 as days_analyzed
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
""")

display(summary_df)

# COMMAND ----------

# DBTITLE 1,Calculate Daily Average
summary_data = summary_df.collect()[0]
total_spend = summary_data['total_spend']
days_analyzed = summary_data['days_analyzed']
avg_daily_spend = total_spend / days_analyzed if days_analyzed > 0 else 0

print(f"Total Spend: ${total_spend:,.2f}")
print(f"Days Analyzed: {days_analyzed}")
print(f"Average Daily Spend: ${avg_daily_spend:,.2f}")
print(f"Projected Monthly Spend: ${avg_daily_spend * 30:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Daily Spending Trends
# MAGIC Track spending patterns over time

# COMMAND ----------

# DBTITLE 1,Daily Spend Time Series
daily_spend_df = spark.sql(f"""
SELECT
  usage_date,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
GROUP BY usage_date
ORDER BY usage_date
""")

display(daily_spend_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cost Breakdown by Product
# MAGIC Understand which products drive costs

# COMMAND ----------

# DBTITLE 1,Product SKU Breakdown
product_df = spark.sql(f"""
SELECT
  u.sku_name as product,
  COUNT(DISTINCT workspace_id) as workspaces_using,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  ROUND(100.0 * SUM(usage_quantity * COALESCE(p.pricing.default, 0)) / SUM(SUM(usage_quantity * COALESCE(p.pricing.default, 0))) OVER (), 2) as percentage
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
GROUP BY u.sku_name
ORDER BY total_spend DESC
""")

display(product_df)

# COMMAND ----------

# DBTITLE 1,High-Level Product Categories
product_category_df = spark.sql(f"""
SELECT
  CASE
    WHEN u.sku_name LIKE '%SQL%' OR u.sku_name LIKE '%WAREHOUSE%' THEN 'SQL & Warehousing'
    WHEN u.sku_name LIKE '%ALL_PURPOSE%' THEN 'Interactive Compute'
    WHEN u.sku_name LIKE '%JOBS%' OR u.sku_name LIKE '%AUTOMATED%' THEN 'Jobs Compute'
    WHEN u.sku_name LIKE '%DLT%' OR u.sku_name LIKE '%PIPELINE%' THEN 'Delta Live Tables'
    WHEN u.sku_name LIKE '%INFERENCE%' OR u.sku_name LIKE '%MODEL%' THEN 'Model Serving'
    WHEN u.sku_name LIKE '%VECTOR_SEARCH%' THEN 'Vector Search'
    ELSE 'Other'
  END as category,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  ROUND(100.0 * SUM(usage_quantity * COALESCE(p.pricing.default, 0)) / SUM(SUM(usage_quantity * COALESCE(p.pricing.default, 0))) OVER (), 2) as percentage
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
GROUP BY category
ORDER BY total_spend DESC
""")

display(product_category_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Workspace-Level Analysis
# MAGIC Identify which workspaces drive the most costs

# COMMAND ----------

# DBTITLE 1,Top Workspaces by Spend
workspace_df = spark.sql(f"""
SELECT
  workspace_id,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  ROUND(100.0 * SUM(usage_quantity * COALESCE(p.pricing.default, 0)) / SUM(SUM(usage_quantity * COALESCE(p.pricing.default, 0))) OVER (), 2) as percentage,
  COUNT(DISTINCT usage_date) as days_active
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
GROUP BY workspace_id
ORDER BY total_spend DESC
LIMIT 20
""")

display(workspace_df)

# COMMAND ----------

# DBTITLE 1,Workspace Spend Over Time
workspace_daily_df = spark.sql(f"""
SELECT
  workspace_id,
  usage_date,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as daily_spend
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
GROUP BY workspace_id, usage_date
ORDER BY workspace_id, usage_date
""")

display(workspace_daily_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. SQL Warehousing Analysis
# MAGIC Deep dive into SQL compute costs

# COMMAND ----------

# DBTITLE 1,SQL Warehouse Costs
sql_warehouse_df = spark.sql(f"""
SELECT
  usage_metadata.warehouse_id,
  u.sku_name,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  COUNT(DISTINCT usage_date) as days_active,
  COUNT(DISTINCT workspace_id) as workspaces
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
  AND (u.sku_name LIKE '%SQL%' OR u.sku_name LIKE '%WAREHOUSE%')
  AND usage_metadata.warehouse_id IS NOT NULL
GROUP BY usage_metadata.warehouse_id, u.sku_name
ORDER BY total_spend DESC
LIMIT 50
""")

display(sql_warehouse_df)

# COMMAND ----------

# DBTITLE 1,SQL vs Genie Attribution
# Note: Genie queries don't have a special SKU - we identify them via system.query.history
# This uses temporal price joins, product taxonomy, and work time for better accuracy
sql_breakdown_df = spark.sql(f"""
WITH warehouse_costs AS (
  -- Get total costs per warehouse with temporal price join
  SELECT
    usage_metadata.warehouse_id,
    SUM(usage_quantity) as total_dbus,
    SUM(usage_quantity * COALESCE(p.pricing.effective_list.default, p.pricing.default, 0)) as total_cost
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
    AND u.cloud = p.cloud
    -- Temporal join: respect historical price changes
    AND u.usage_end_time >= p.price_start_time
    AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
  WHERE u.usage_date >= '{start_date}'
    AND u.usage_date <= '{end_date}'
    AND u.usage_quantity > 0
    AND u.billing_origin_product = 'SQL'  -- Use product taxonomy instead of SKU LIKE
    AND usage_metadata.warehouse_id IS NOT NULL
  GROUP BY usage_metadata.warehouse_id
),
query_attribution AS (
  -- Identify Genie vs other DBSQL queries and calculate work time
  -- Work time = task execution + compilation + fetch (excludes waiting)
  SELECT
    compute.warehouse_id,
    CASE
      WHEN query_source.genie_space_id IS NOT NULL THEN 'Genie'
      WHEN client_application ILIKE '%Genie%' THEN 'Genie'
      ELSE 'Other DBSQL'
    END as channel,
    COUNT(*) as query_count,
    -- Calculate effective work seconds
    SUM(
      (COALESCE(total_task_duration_ms, 0)
       + COALESCE(compilation_duration_ms, 0)
       + COALESCE(result_fetch_duration_ms, 0)) / 1000.0
    ) as total_work_seconds
  FROM system.query.history
  WHERE start_time >= '{start_date}'
    AND end_time <= '{end_date}'
    AND compute.warehouse_id IS NOT NULL
    AND execution_status = 'FINISHED'  -- Only finished queries
    AND total_task_duration_ms > 0
  GROUP BY compute.warehouse_id, channel
),
warehouse_totals AS (
  -- Get total work time per warehouse for proportional allocation
  SELECT
    warehouse_id,
    SUM(total_work_seconds) as warehouse_total_seconds
  FROM query_attribution
  GROUP BY warehouse_id
),
allocated_costs AS (
  -- Allocate warehouse costs proportionally by query work time
  SELECT
    qa.channel,
    qa.warehouse_id,
    qa.query_count,
    qa.total_work_seconds,
    wc.total_cost,
    -- Proportional cost allocation based on work time
    (qa.total_work_seconds / NULLIF(wt.warehouse_total_seconds, 0)) * wc.total_cost as allocated_cost,
    (qa.total_work_seconds / NULLIF(wt.warehouse_total_seconds, 0)) * wc.total_dbus as allocated_dbus
  FROM query_attribution qa
  JOIN warehouse_costs wc ON qa.warehouse_id = wc.warehouse_id
  JOIN warehouse_totals wt ON qa.warehouse_id = wt.warehouse_id
)
SELECT
  channel,
  SUM(query_count) as total_queries,
  SUM(total_work_seconds) as total_work_seconds,
  SUM(allocated_dbus) as estimated_dbus,
  SUM(allocated_cost) as estimated_cost,
  ROUND(100.0 * SUM(allocated_cost) / SUM(SUM(allocated_cost)) OVER (), 2) as percentage
FROM allocated_costs
GROUP BY channel
ORDER BY estimated_cost DESC
COMMENT 'Simplified allocation. For production-grade accuracy, use warehouse ON/OFF windows from system.compute.warehouse_events and hourly grain matching per DBSQL Cost Per Query MV pattern.'
""")

display(sql_breakdown_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Interactive Compute Analysis
# MAGIC Analyze all-purpose cluster usage by users and notebooks

# COMMAND ----------

# DBTITLE 1,Interactive Compute by User
interactive_user_df = spark.sql(f"""
SELECT
  identity_metadata.run_as as user_email,
  COUNT(DISTINCT usage_metadata.cluster_id) as clusters_used,
  COUNT(DISTINCT usage_metadata.notebook_id) as notebooks_run,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
  AND u.sku_name LIKE '%ALL_PURPOSE%'
  AND identity_metadata.run_as IS NOT NULL
GROUP BY identity_metadata.run_as
ORDER BY total_spend DESC
LIMIT 50
""")

display(interactive_user_df)

# COMMAND ----------

# DBTITLE 1,Top Notebooks by Cost
interactive_notebook_df = spark.sql(f"""
SELECT
  usage_metadata.notebook_path,
  identity_metadata.run_as as user_email,
  COUNT(DISTINCT usage_metadata.cluster_id) as clusters_used,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  COUNT(DISTINCT usage_date) as days_active
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
  AND u.sku_name LIKE '%ALL_PURPOSE%'
  AND usage_metadata.notebook_path IS NOT NULL
GROUP BY usage_metadata.notebook_path, identity_metadata.run_as
ORDER BY total_spend DESC
LIMIT 50
""")

display(interactive_notebook_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Jobs Compute Analysis
# MAGIC Analyze automated job costs

# COMMAND ----------

# DBTITLE 1,Jobs Costs by Run Name
jobs_df = spark.sql(f"""
SELECT
  usage_metadata.job_id,
  usage_metadata.run_name,
  COUNT(DISTINCT usage_metadata.cluster_id) as clusters_used,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  COUNT(DISTINCT usage_date) as days_active
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
  AND (u.sku_name LIKE '%JOBS%' OR u.sku_name LIKE '%AUTOMATED%')
  AND usage_metadata.job_id IS NOT NULL
GROUP BY usage_metadata.job_id, usage_metadata.run_name
ORDER BY total_spend DESC
LIMIT 50
""")

display(jobs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Delta Live Tables (DLT) Analysis
# MAGIC Analyze pipeline costs

# COMMAND ----------

# DBTITLE 1,DLT Pipeline Costs
dlt_df = spark.sql(f"""
SELECT
  usage_metadata.dlt_pipeline_id as pipeline_id,
  usage_metadata.dlt_update_id as update_id,
  u.sku_name,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  COUNT(DISTINCT usage_date) as days_active
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
  AND (u.sku_name LIKE '%DLT%' OR u.sku_name LIKE '%PIPELINE%')
  AND usage_metadata.dlt_pipeline_id IS NOT NULL
GROUP BY usage_metadata.dlt_pipeline_id, usage_metadata.dlt_update_id, u.sku_name
ORDER BY total_spend DESC
LIMIT 50
""")

display(dlt_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Batch vs Streaming Cost Analysis
# MAGIC Break down DLT and Jobs costs by execution mode

# COMMAND ----------

# DBTITLE 1,Batch vs Streaming Breakdown
# NOTE: DLT execution mode determined from pipeline settings.continuous
# Jobs execution mode is left as 'Batch (Estimated)' - join to job telemetry for accuracy
batch_streaming_df = spark.sql(f"""
WITH usage_base AS (
  SELECT
    u.workspace_id,
    u.usage_date,
    u.usage_start_time,
    u.usage_end_time,
    u.cloud,
    u.sku_name,
    u.billing_origin_product,
    u.usage_quantity,
    u.usage_unit,
    u.usage_metadata.dlt_pipeline_id AS dlt_pipeline_id,
    u.usage_metadata.job_id AS job_id
  FROM system.billing.usage u
  WHERE u.usage_date >= '{start_date}'
    AND u.usage_date <= '{end_date}'
    AND u.usage_unit = 'DBU'
    AND u.billing_origin_product IN ('DLT', 'JOBS')
),
priced AS (
  SELECT
    b.*,
    CAST(lp.pricing.effective_list.default AS DOUBLE) AS unit_price,
    b.usage_quantity * CAST(lp.pricing.effective_list.default AS DOUBLE) AS list_cost
  FROM usage_base b
  LEFT JOIN system.billing.list_prices lp
    ON lp.cloud = b.cloud
    AND lp.sku_name = b.sku_name
    AND b.usage_start_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR b.usage_end_time <= lp.price_end_time)
),
pipelines_asof AS (
  SELECT
    p.workspace_id,
    p.pipeline_id,
    p.settings.continuous AS is_continuous,
    p.change_time
  FROM system.lakeflow.pipelines p
),
pipelines_latest_by_usage AS (
  SELECT
    u.workspace_id,
    u.dlt_pipeline_id AS pipeline_id,
    FIRST(p.is_continuous, true) AS is_continuous
  FROM priced u
  LEFT JOIN pipelines_asof p
    ON p.workspace_id = u.workspace_id
    AND p.pipeline_id = u.dlt_pipeline_id
    AND p.change_time <= u.usage_start_time
  WHERE u.dlt_pipeline_id IS NOT NULL
  GROUP BY u.workspace_id, u.dlt_pipeline_id
),
classified AS (
  SELECT
    CASE
      WHEN t.billing_origin_product = 'DLT' THEN 'DLT'
      WHEN t.billing_origin_product = 'JOBS' THEN 'Jobs'
      ELSE t.billing_origin_product
    END AS workload_type,
    CASE
      WHEN t.billing_origin_product = 'DLT' AND pl.is_continuous IS TRUE THEN 'Streaming'
      WHEN t.billing_origin_product = 'DLT' AND pl.is_continuous IS FALSE THEN 'Batch'
      WHEN t.billing_origin_product = 'JOBS' THEN 'Batch (Estimated)'
      ELSE 'Unknown'
    END AS execution_mode,
    t.usage_quantity,
    t.list_cost
  FROM priced t
  LEFT JOIN pipelines_latest_by_usage pl
    ON pl.workspace_id = t.workspace_id
    AND pl.pipeline_id = t.dlt_pipeline_id
)
SELECT
  workload_type,
  execution_mode,
  COUNT(*) AS usage_records,
  SUM(usage_quantity) AS total_dbus,
  SUM(list_cost) AS total_spend,
  ROUND(100.0 * SUM(list_cost) / SUM(SUM(list_cost)) OVER (), 2) AS percentage
FROM classified
GROUP BY workload_type, execution_mode
ORDER BY total_spend DESC
""")

display(batch_streaming_df)

# COMMAND ----------

# DBTITLE 1,Batch vs Streaming Trend Over Time
# NOTE: DLT execution mode determined from pipeline settings.continuous
batch_streaming_trend_df = spark.sql(f"""
WITH usage_base AS (
  SELECT
    u.workspace_id,
    u.usage_date,
    u.usage_start_time,
    u.usage_end_time,
    u.cloud,
    u.sku_name,
    u.billing_origin_product,
    u.usage_quantity,
    u.usage_unit,
    u.usage_metadata.dlt_pipeline_id AS dlt_pipeline_id,
    u.usage_metadata.job_id AS job_id
  FROM system.billing.usage u
  WHERE u.usage_date >= '{start_date}'
    AND u.usage_date <= '{end_date}'
    AND u.usage_unit = 'DBU'
    AND u.billing_origin_product IN ('DLT', 'JOBS')
),
priced AS (
  SELECT
    b.*,
    b.usage_quantity * CAST(lp.pricing.effective_list.default AS DOUBLE) AS list_cost
  FROM usage_base b
  LEFT JOIN system.billing.list_prices lp
    ON lp.cloud = b.cloud
    AND lp.sku_name = b.sku_name
    AND b.usage_start_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR b.usage_end_time <= lp.price_end_time)
),
pipelines_asof AS (
  SELECT
    p.workspace_id,
    p.pipeline_id,
    p.settings.continuous AS is_continuous,
    p.change_time
  FROM system.lakeflow.pipelines p
),
pipelines_latest_by_usage AS (
  SELECT
    u.workspace_id,
    u.dlt_pipeline_id AS pipeline_id,
    FIRST(p.is_continuous, true) AS is_continuous
  FROM priced u
  LEFT JOIN pipelines_asof p
    ON p.workspace_id = u.workspace_id
    AND p.pipeline_id = u.dlt_pipeline_id
    AND p.change_time <= u.usage_start_time
  WHERE u.dlt_pipeline_id IS NOT NULL
  GROUP BY u.workspace_id, u.dlt_pipeline_id
),
classified AS (
  SELECT
    t.usage_date,
    CASE
      WHEN t.billing_origin_product = 'DLT' THEN 'DLT'
      WHEN t.billing_origin_product = 'JOBS' THEN 'Jobs'
      ELSE t.billing_origin_product
    END AS workload_type,
    CASE
      WHEN t.billing_origin_product = 'DLT' AND pl.is_continuous IS TRUE THEN 'Streaming'
      WHEN t.billing_origin_product = 'DLT' AND pl.is_continuous IS FALSE THEN 'Batch'
      WHEN t.billing_origin_product = 'JOBS' THEN 'Batch (Estimated)'
      ELSE 'Unknown'
    END AS execution_mode,
    t.list_cost
  FROM priced t
  LEFT JOIN pipelines_latest_by_usage pl
    ON pl.workspace_id = t.workspace_id
    AND pl.pipeline_id = t.dlt_pipeline_id
)
SELECT
  usage_date,
  workload_type,
  execution_mode,
  SUM(list_cost) AS daily_spend
FROM classified
WHERE workload_type IN ('DLT', 'Jobs')
GROUP BY usage_date, workload_type, execution_mode
ORDER BY usage_date, workload_type, execution_mode
""")

display(batch_streaming_trend_df)

# COMMAND ----------

# DBTITLE 1,Top DLT Pipelines by Cost
# NOTE: DLT execution mode determined from pipeline settings.continuous
top_dlt_df = spark.sql(f"""
WITH usage_base AS (
  SELECT
    u.workspace_id,
    u.usage_date,
    u.usage_start_time,
    u.usage_end_time,
    u.cloud,
    u.sku_name,
    u.usage_quantity,
    u.usage_metadata.dlt_pipeline_id AS dlt_pipeline_id,
    u.usage_metadata.dlt_update_id AS dlt_update_id
  FROM system.billing.usage u
  WHERE u.usage_date >= '{start_date}'
    AND u.usage_date <= '{end_date}'
    AND u.usage_unit = 'DBU'
    AND u.billing_origin_product = 'DLT'
    AND u.usage_metadata.dlt_pipeline_id IS NOT NULL
),
priced AS (
  SELECT
    b.*,
    b.usage_quantity * CAST(lp.pricing.effective_list.default AS DOUBLE) AS list_cost
  FROM usage_base b
  LEFT JOIN system.billing.list_prices lp
    ON lp.cloud = b.cloud
    AND lp.sku_name = b.sku_name
    AND b.usage_start_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR b.usage_end_time <= lp.price_end_time)
),
pipelines_asof AS (
  SELECT
    p.workspace_id,
    p.pipeline_id,
    p.settings.continuous AS is_continuous,
    p.change_time
  FROM system.lakeflow.pipelines p
),
pipelines_latest_by_usage AS (
  SELECT
    u.workspace_id,
    u.dlt_pipeline_id AS pipeline_id,
    FIRST(p.is_continuous, true) AS is_continuous
  FROM priced u
  LEFT JOIN pipelines_asof p
    ON p.workspace_id = u.workspace_id
    AND p.pipeline_id = u.dlt_pipeline_id
    AND p.change_time <= u.usage_start_time
  GROUP BY u.workspace_id, u.dlt_pipeline_id
)
SELECT
  t.dlt_pipeline_id AS pipeline_id,
  t.dlt_update_id AS update_id,
  t.sku_name,
  CASE
    WHEN pl.is_continuous IS TRUE THEN 'Streaming'
    WHEN pl.is_continuous IS FALSE THEN 'Batch'
    ELSE 'Unknown'
  END AS execution_mode,
  SUM(t.usage_quantity) AS total_dbus,
  SUM(t.list_cost) AS total_spend,
  COUNT(DISTINCT t.usage_date) AS days_active
FROM priced t
LEFT JOIN pipelines_latest_by_usage pl
  ON pl.workspace_id = t.workspace_id
  AND pl.pipeline_id = t.dlt_pipeline_id
GROUP BY t.dlt_pipeline_id, t.dlt_update_id, t.sku_name, execution_mode
ORDER BY total_spend DESC
LIMIT 20
""")

display(top_dlt_df)

# COMMAND ----------

# DBTITLE 1,Top Jobs by Cost
# NOTE: Jobs execution mode is 'Batch (Estimated)' - join to job telemetry for streaming accuracy
top_jobs_df = spark.sql(f"""
WITH usage_base AS (
  SELECT
    u.usage_date,
    u.usage_start_time,
    u.usage_end_time,
    u.cloud,
    u.sku_name,
    u.usage_quantity,
    u.usage_metadata.job_id AS job_id,
    u.usage_metadata.run_name AS run_name,
    u.usage_metadata.cluster_id AS cluster_id
  FROM system.billing.usage u
  WHERE u.usage_date >= '{start_date}'
    AND u.usage_date <= '{end_date}'
    AND u.usage_unit = 'DBU'
    AND u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL
),
priced AS (
  SELECT
    b.*,
    b.usage_quantity * CAST(lp.pricing.effective_list.default AS DOUBLE) AS list_cost
  FROM usage_base b
  LEFT JOIN system.billing.list_prices lp
    ON lp.cloud = b.cloud
    AND lp.sku_name = b.sku_name
    AND b.usage_start_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR b.usage_end_time <= lp.price_end_time)
)
SELECT
  job_id,
  run_name,
  'Batch (Estimated)' AS execution_mode,
  COUNT(DISTINCT cluster_id) AS clusters_used,
  SUM(usage_quantity) AS total_dbus,
  SUM(list_cost) AS total_spend,
  COUNT(DISTINCT usage_date) AS days_active
FROM priced
GROUP BY job_id, run_name
ORDER BY total_spend DESC
LIMIT 20
""")

display(top_jobs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cost Anomalies
# MAGIC Identify unusual spending patterns

# COMMAND ----------

# DBTITLE 1,Day-over-Day Spend Changes
anomaly_df = spark.sql(f"""
WITH daily_stats AS (
  SELECT
    usage_date,
    SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as daily_spend
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
    AND u.cloud = p.cloud
    AND p.price_end_time IS NULL
  WHERE u.usage_date >= '{start_date}'
    AND u.usage_date <= '{end_date}'
    AND u.usage_quantity > 0
  GROUP BY usage_date
),
with_lag AS (
  SELECT
    usage_date,
    daily_spend,
    LAG(daily_spend) OVER (ORDER BY usage_date) as prev_day_spend
  FROM daily_stats
)
SELECT
  usage_date,
  daily_spend,
  prev_day_spend,
  daily_spend - prev_day_spend as change_amount,
  ROUND(100.0 * (daily_spend - prev_day_spend) / NULLIF(prev_day_spend, 0), 2) as change_percent
FROM with_lag
WHERE prev_day_spend IS NOT NULL
ORDER BY ABS(change_percent) DESC
LIMIT 20
""")

display(anomaly_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Export Summary Report

# COMMAND ----------

# DBTITLE 1,Generate Executive Summary
exec_summary = spark.sql(f"""
WITH totals AS (
  SELECT
    SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
    SUM(usage_quantity) as total_dbus
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
    AND u.cloud = p.cloud
    AND p.price_end_time IS NULL
  WHERE u.usage_date >= '{start_date}'
    AND u.usage_date <= '{end_date}'
    AND u.usage_quantity > 0
),
top_product AS (
  SELECT u.sku_name, SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as spend
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND p.price_end_time IS NULL
  WHERE u.usage_date >= '{start_date}' AND u.usage_date <= '{end_date}' AND u.usage_quantity > 0
  GROUP BY u.sku_name
  ORDER BY spend DESC
  LIMIT 1
),
top_workspace AS (
  SELECT workspace_id, SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as spend
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND p.price_end_time IS NULL
  WHERE u.usage_date >= '{start_date}' AND u.usage_date <= '{end_date}' AND u.usage_quantity > 0
  GROUP BY workspace_id
  ORDER BY spend DESC
  LIMIT 1
)
SELECT
  '{start_date}' as analysis_start_date,
  '{end_date}' as analysis_end_date,
  t.total_spend,
  t.total_dbus,
  tp.sku_name as top_product,
  tp.spend as top_product_spend,
  tw.workspace_id as top_workspace,
  tw.spend as top_workspace_spend
FROM totals t
CROSS JOIN top_product tp
CROSS JOIN top_workspace tw
""")

display(exec_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook provides a comprehensive cost analysis using Databricks system tables:
# MAGIC
# MAGIC 1. **Account Summary** - Overall spend and DBU consumption
# MAGIC 2. **Daily Trends** - Spending patterns over time
# MAGIC 3. **Product Breakdown** - Costs by SKU and category
# MAGIC 4. **Workspace Analysis** - Per-workspace cost attribution
# MAGIC 5. **SQL Warehousing** - SQL and Genie costs (using query.history for attribution)
# MAGIC 6. **Interactive Compute** - User and notebook attribution
# MAGIC 7. **Jobs Compute** - Automated job costs
# MAGIC 8. **Delta Live Tables** - Pipeline costs
# MAGIC 9. **Batch vs Streaming** - DLT and Jobs execution mode breakdown
# MAGIC 10. **Anomaly Detection** - Unusual spending patterns
# MAGIC 11. **Executive Summary** - High-level metrics
