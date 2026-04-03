# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Cost Analysis
# MAGIC
# MAGIC This notebook provides comprehensive cost analysis for Databricks workspaces and accounts.
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - `system.billing.usage` - All usage records with DBU consumption
# MAGIC - `system.billing.list_prices` - Current pricing by SKU
# MAGIC - `system.compute.clusters` - Cluster configurations
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
  sku_name as product,
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
GROUP BY sku_name
ORDER BY total_spend DESC
""")

display(product_df)

# COMMAND ----------

# DBTITLE 1,High-Level Product Categories
product_category_df = spark.sql(f"""
SELECT
  CASE
    WHEN sku_name LIKE '%SQL%' OR sku_name LIKE '%WAREHOUSE%' THEN 'SQL & Warehousing'
    WHEN sku_name LIKE '%ALL_PURPOSE%' THEN 'Interactive Compute'
    WHEN sku_name LIKE '%JOBS%' OR sku_name LIKE '%AUTOMATED%' THEN 'Jobs Compute'
    WHEN sku_name LIKE '%DLT%' OR sku_name LIKE '%PIPELINE%' THEN 'Delta Live Tables'
    WHEN sku_name LIKE '%INFERENCE%' OR sku_name LIKE '%MODEL%' THEN 'Model Serving'
    WHEN sku_name LIKE '%VECTOR_SEARCH%' THEN 'Vector Search'
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
  sku_name,
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
  AND (sku_name LIKE '%SQL%' OR sku_name LIKE '%WAREHOUSE%')
  AND usage_metadata.warehouse_id IS NOT NULL
GROUP BY usage_metadata.warehouse_id, sku_name
ORDER BY total_spend DESC
LIMIT 50
""")

display(sql_warehouse_df)

# COMMAND ----------

# DBTITLE 1,SQL vs Genie Attribution
sql_breakdown_df = spark.sql(f"""
SELECT
  CASE
    WHEN sku_name LIKE '%GENIE%' THEN 'Genie'
    WHEN sku_name LIKE '%SQL%' OR sku_name LIKE '%WAREHOUSE%' THEN 'SQL Warehouse'
    ELSE 'Other'
  END as sql_type,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend,
  COUNT(DISTINCT usage_metadata.warehouse_id) as warehouse_count
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud = p.cloud
  AND p.price_end_time IS NULL
WHERE u.usage_date >= '{start_date}'
  AND u.usage_date <= '{end_date}'
  AND u.usage_quantity > 0
  AND (sku_name LIKE '%SQL%' OR sku_name LIKE '%WAREHOUSE%' OR sku_name LIKE '%GENIE%')
GROUP BY sql_type
ORDER BY total_spend DESC
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
  AND sku_name LIKE '%ALL_PURPOSE%'
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
  AND sku_name LIKE '%ALL_PURPOSE%'
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

# DBTITLE 1,Jobs by Run Name
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
  AND (sku_name LIKE '%JOBS%' OR sku_name LIKE '%AUTOMATED%')
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
  sku_name,
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
  AND (sku_name LIKE '%DLT%' OR sku_name LIKE '%PIPELINE%')
  AND usage_metadata.dlt_pipeline_id IS NOT NULL
GROUP BY usage_metadata.dlt_pipeline_id, usage_metadata.dlt_update_id, sku_name
ORDER BY total_spend DESC
LIMIT 50
""")

display(dlt_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cost Anomalies
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
# MAGIC ## 10. Export Summary Report

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
  SELECT sku_name, SUM(usage_quantity * COALESCE(p.pricing.default, 0)) as spend
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND p.price_end_time IS NULL
  WHERE u.usage_date >= '{start_date}' AND u.usage_date <= '{end_date}' AND u.usage_quantity > 0
  GROUP BY sku_name
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
# MAGIC 5. **SQL Warehousing** - SQL and Genie costs
# MAGIC 6. **Interactive Compute** - User and notebook attribution
# MAGIC 7. **Jobs Compute** - Automated job costs
# MAGIC 8. **Delta Live Tables** - Pipeline costs
# MAGIC 9. **Anomaly Detection** - Unusual spending patterns
# MAGIC 10. **Executive Summary** - High-level metrics
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Adjust the date range in the Configuration section
# MAGIC - Filter by specific workspace_id for single-workspace analysis
# MAGIC - Export results to Delta tables for historical tracking
# MAGIC - Set up alerts for cost anomalies
