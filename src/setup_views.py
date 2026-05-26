# Databricks notebook source
# MAGIC %md
# MAGIC # IDP Cost Tracking — view setup
# MAGIC
# MAGIC Creates the schema and 5 views that power the IDP Cost Tracker dashboard.
# MAGIC Run via the bundle job `setup_idp_views`. Idempotent.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("lookback_30d", "30")
dbutils.widgets.text("lookback_60d", "60")
dbutils.widgets.text("lookback_90d", "90")

catalog       = dbutils.widgets.get("catalog")
schema        = dbutils.widgets.get("schema")
lookback_30d  = int(dbutils.widgets.get("lookback_30d"))
lookback_60d  = int(dbutils.widgets.get("lookback_60d"))
lookback_90d  = int(dbutils.widgets.get("lookback_90d"))

assert catalog, "catalog parameter is required"
assert schema,  "schema parameter is required"

print(f"Deploying IDP cost tracking views to {catalog}.{schema}")
print(f"Lookback windows: short={lookback_30d}d trend={lookback_60d}d user_list={lookback_90d}d")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

# v_idp_usage_facts — atomic per-record fact view with $ cost from list_prices
spark.sql(f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.v_idp_usage_facts AS
SELECT
  u.account_id,
  u.workspace_id,
  u.usage_date,
  u.usage_start_time,
  u.usage_end_time,
  u.sku_name,
  u.cloud,
  u.product_features.ai_functions.ai_function           AS ai_function,
  u.usage_metadata.endpoint_name                        AS endpoint_name,
  u.usage_metadata.warehouse_id                         AS warehouse_id,
  u.usage_metadata.job_id                               AS job_id,
  u.usage_metadata.job_run_id                           AS job_run_id,
  u.usage_metadata.job_name                             AS job_name,
  u.usage_metadata.notebook_id                          AS notebook_id,
  u.usage_metadata.notebook_path                        AS notebook_path,
  u.usage_metadata.dlt_pipeline_id                      AS dlt_pipeline_id,
  u.usage_metadata.dlt_update_id                        AS dlt_update_id,
  u.identity_metadata.run_as                            AS run_as,
  u.identity_metadata.created_by                        AS created_by,
  u.custom_tags,
  u.usage_quantity                                      AS dbus,
  p.pricing.effective_list.default                      AS price_per_dbu_usd,
  u.usage_quantity * p.pricing.effective_list.default   AS cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
  AND u.cloud   = p.cloud
  AND u.usage_unit = p.usage_unit
  AND u.usage_start_time >= p.price_start_time
  AND u.usage_start_time <  COALESCE(p.price_end_time, TIMESTAMP'9999-12-31')
WHERE u.billing_origin_product = 'AI_FUNCTIONS'
  AND u.record_type = 'ORIGINAL'
  AND u.product_features.ai_functions.ai_function IS NOT NULL
""")

# COMMAND ----------

# v_idp_daily_summary — daily rollup per workspace × function
spark.sql(f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.v_idp_daily_summary AS
SELECT
  usage_date,
  workspace_id,
  ai_function,
  COUNT(*)                AS usage_records,
  SUM(dbus)               AS dbus,
  SUM(cost_usd)           AS cost_usd,
  COUNT(DISTINCT run_as)  AS distinct_users,
  COUNT(DISTINCT job_id)  AS distinct_jobs,
  COUNT(DISTINCT dlt_pipeline_id) AS distinct_pipelines,
  COUNT(DISTINCT warehouse_id)    AS distinct_warehouses
FROM `{catalog}`.`{schema}`.v_idp_usage_facts
GROUP BY usage_date, workspace_id, ai_function
""")

# COMMAND ----------

# v_idp_by_workload — attribution to job / pipeline / warehouse / notebook / api_sdk
# Uses custom_tags as fallback for interactive notebooks on all-purpose clusters
# and for Databricks Connect / SDK / REST API direct invocations.
spark.sql(f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.v_idp_by_workload AS
SELECT
  usage_date,
  workspace_id,
  ai_function,
  CASE
    WHEN job_id IS NOT NULL                                        THEN 'JOB'
    WHEN dlt_pipeline_id IS NOT NULL                               THEN 'PIPELINE'
    WHEN warehouse_id IS NOT NULL                                  THEN 'WAREHOUSE'
    WHEN notebook_id IS NOT NULL                                   THEN 'NOTEBOOK'
    WHEN custom_tags['DatabricksScgwWorkloadType'] = 'Notebooks'   THEN 'NOTEBOOK'
    WHEN custom_tags['DatabricksScgwWorkloadType'] = 'Jobs'        THEN 'JOB'
    WHEN custom_tags['DatabricksScgwWorkloadType'] = 'Unspecified' THEN 'API_SDK'
    ELSE 'OTHER'
  END AS workload_type,
  COALESCE(
    job_id, dlt_pipeline_id, warehouse_id, notebook_id,
    custom_tags['DatabricksScgwClusterId'], 'unattributed'
  ) AS workload_id,
  COALESCE(job_name, notebook_path) AS workload_label,
  run_as,
  COUNT(*)                AS usage_records,
  SUM(dbus)               AS dbus,
  SUM(cost_usd)           AS cost_usd
FROM `{catalog}`.`{schema}`.v_idp_usage_facts
GROUP BY ALL
""")

# COMMAND ----------

# v_idp_by_user — per-user chargeback
spark.sql(f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.v_idp_by_user AS
SELECT
  usage_date,
  workspace_id,
  run_as,
  ai_function,
  COUNT(*)                AS usage_records,
  SUM(dbus)               AS dbus,
  SUM(cost_usd)           AS cost_usd
FROM `{catalog}`.`{schema}`.v_idp_usage_facts
WHERE run_as IS NOT NULL
GROUP BY ALL
""")

# COMMAND ----------

# v_idp_warehouse_queries — DBSQL-originated AI function calls (statement-level)
spark.sql(f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.v_idp_warehouse_queries AS
SELECT
  qh.workspace_id,
  qh.statement_id,
  qh.executed_by,
  qh.compute.warehouse_id  AS warehouse_id,
  CAST(qh.start_time AS DATE) AS usage_date,
  qh.start_time,
  qh.end_time,
  qh.total_duration_ms / 1000.0 AS duration_seconds,
  qh.execution_status,
  CASE
    WHEN qh.statement_text ILIKE '%ai_parse_document(%' AND qh.statement_text ILIKE '%ai_extract(%' THEN 'BOTH'
    WHEN qh.statement_text ILIKE '%ai_parse_document(%' THEN 'AI_PARSE_DOCUMENT'
    WHEN qh.statement_text ILIKE '%ai_extract(%'        THEN 'AI_EXTRACT'
    WHEN qh.statement_text ILIKE '%ai_classify(%'       THEN 'AI_CLASSIFY'
  END AS ai_function_detected,
  qh.statement_text,
  qh.read_rows,
  qh.produced_rows
FROM system.query.history qh
WHERE qh.start_time >= current_date() - INTERVAL {lookback_90d} DAYS
  AND qh.compute.type = 'WAREHOUSE'
  AND (qh.statement_text ILIKE '%ai_parse_document(%'
       OR qh.statement_text ILIKE '%ai_extract(%'
       OR qh.statement_text ILIKE '%ai_classify(%')
""")

# COMMAND ----------

print(f"OK: 5 views created in {catalog}.{schema}")
