# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

# Imports
dbutils.widgets.text("environment", "")

from generic.config.environment import get_environment
# ==========================================
# SSAE BRONZE TABLE CREATION CONTROL/CONFIG TABLES
# ==========================================

# -------------------------------
# PARAMETERS
# -------------------------------
environment = get_environment(
    spark=spark,
    job_environment=dbutils.widgets.get("environment"),
)
catalog = f"cdp_data_gsoc_{environment}"
schema = "control_gsoc"


#spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


# ============================================================
# 1. endpoint_config
# ------------------------------------------------------------
# Purpose:
#   Defines which SSAE endpoints must be called and how.
#   One row per country + endpoint ingestion unit.
#
# Examples:
#   GC_EU / event_history
#   GC_EU / site
#   GC_EU / site_changes
#   GC_EU / events
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.endpoint_config (
    endpoint_id STRING,
    country_code STRING,
    endpoint_name STRING,
    entity_name STRING,
    api_country_code STRING,
    controller_type STRING,
    ingestion_mode STRING,
    load_pattern STRING,
    initial_lookback_days INT,
    initial_window_days INT,
    ssae_api_class_method STRING,
    target_bronze_table STRING,
    max_fetch_size INT,
    is_active BOOLEAN
)
USING DELTA
COMMENT 'Configuration table defining SSAE endpoints to ingest'
""")



# ============================================================
# 2. ingestion_state
# ------------------------------------------------------------
# Purpose:
#   Stores the current validated ingestion position.
#   One active row per country + endpoint.
#
# Important:
#   This table must only be updated after the Bronze write has
#   successfully completed.
#
# Examples:
#   - EventHistory: stores the last validated cursor
#   - xxxChanges: stores the last validated to_datetime
#   - Setup tables: stores the last successful full load timestamp
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ingestion_state (
    endpoint_id STRING,
    initial_load_completed BOOLEAN,
    last_status STRING,
    last_success_bronze_run_id STRING,
    last_success_at TIMESTAMP,
    last_cursor STRING,
    last_from_datetime TIMESTAMP,
    last_to_datetime TIMESTAMP,
    last_records_fetched BIGINT,
    last_records_written BIGINT,
    last_error_message STRING
)
USING DELTA
COMMENT 'Current validated ingestion checkpoint per country and endpoint'
""")



# ============================================================
# 3. ingestion_run_log
# ------------------------------------------------------------
# Purpose:
#   Stores the full execution history of ingestion runs.
#   One row per endpoint execution attempt.
#
# Important:
#   This table is an audit log.
#   It should not be used as the primary source of truth for the
#   next cursor or watermark. ingestion_state is the official
#   checkpoint table.
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ingestion_run_log (
    pipeline_run_id STRING,
    bronze_run_id STRING,
    endpoint_id STRING,
    status STRING,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration_seconds BIGINT,
    input_cursor STRING,
    output_cursor STRING,
    from_datetime TIMESTAMP,
    to_datetime TIMESTAMP,
    records_fetched BIGINT,
    records_written BIGINT,
    api_status_code INT,
    retry_count INT,
    error_type STRING,
    error_message STRING
)
USING DELTA
COMMENT 'Audit log of SSAE ingestion runs'
""")


# ==========================================
# SSAE SILVER TABLE CREATION CONTROL/CONFIG TABLES
# ==========================================

# ============================================================
# 1. silver_config
# ------------------------------------------------------------
# Purpose:
#   Defines how to transform the bronze tables into silver tables
#   One row per country + endpoint ingestion unit.
#
# Examples:
#   GC_EU / event_history
#   GC_EU / site
#   GC_EU / events
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_config (
    silver_task_id STRING,
    country_code STRING,
    entity_name STRING,
    source_bronze_table STRING,
    target_silver_table STRING,
    source_endpoint_id STRING,
    processing_pattern STRING,   -- append, merge_scd1, replace, delete, ...
    processing_order INT,  -- 1,2,3 etc : Example for Sites 1 : sites, 2 site_changes, 3 site_deletes
    primary_key_columns ARRAY<STRING>,
    dedup_order_columns ARRAY<STRING>,
    is_active BOOLEAN
)
USING DELTA
COMMENT 'Configuration table defining how to transform the bronze tables into silver tables'
""")


# ============================================================
# 2. silver_processing_queue
# ------------------------------------------------------------
# Purpose:
#   Working table to know which silver tables to process
#   rows are inserted by the Bronze ingestion in case of success
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_processing_queue (
    
    queue_id STRING,
    bronze_run_id STRING,
    silver_task_id STRING,
    status STRING,          -- PENDING, PROCESSING, SUCCESS, FAILED, SKIPPED, ...
    created_at TIMESTAMP,
    started_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_silver_run_id STRING,
    last_error_message STRING
    
)
USING DELTA
COMMENT 'Silver queue tables to be processed'
""")


# ============================================================
# 3. silver_run_log
# ------------------------------------------------------------
# Purpose:
#   Monitor and stores the full execution history of silver runs
#   One row per endpoint execution attempt.
#
# Important:
#   This table is an audit log.
#   This table should not be used to compute/recompute silver tables
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_run_log (
    pipeline_run_id STRING,
    silver_run_id STRING,
    queue_id STRING,
    silver_task_id STRING,    
    status STRING,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration_seconds BIGINT,
    records_read BIGINT,
    records_inserted BIGINT,
    records_updated BIGINT,
    records_deleted BIGINT,
    error_message STRING
)
USING DELTA
COMMENT 'Audit log of silver runs'
""")

# ==========================================
# SSAE RESTRICTION TABLE CREATION CONTROL/CONFIG TABLES
# ==========================================

# ============================================================
# 1. restriction_processing_queue
# ------------------------------------------------------------
# Purpose:
#   Working table to manage restrictions when a restriction change is received (ToInclude or ToExclude)
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.restriction_processing_queue (    
    restriction_queue_id STRING,
    pipeline_run_id STRING,
    bronze_run_id STRING,
    site_no INT,
    restriction_action STRING,      -- INCLUDE, EXCLUDE
    status STRING,                  -- PENDING, PROCESSING, SUCCESS, FAILED, SKIPPED
    source_endpoint_id STRING,      -- e.g. GSOC_restriction_changes    
    created_at TIMESTAMP,
    started_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_error_message STRING
)
USING DELTA
COMMENT 'Restriction queue tables to be processed - Specific'
""")

# ============================================================
# 2. restriction_processing_log
# ------------------------------------------------------------
# Purpose:
#   Monitor and stores the full execution history of restrictions runs
#   One row per restriction execution attempt and site_no/target_entity.
#
# Important:
#   This table is an audit log.
#   This table should not be used to compute/recompute silver tables
# ============================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.restriction_processing_log (
    pipeline_run_id STRING,
    restriction_run_id STRING,
    restriction_queue_id STRING, 
    bronze_run_id STRING,
    site_no INT,
    restriction_action STRING,      -- INCLUDE, EXCLUDE
    target_entity STRING,           -- site, system, event_history, alarm_incident, etc.
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration_seconds INT,
    status STRING,                  -- SUCCESS, FAILED, SKIPPED
    records_affected BIGINT,
    error_message STRING
)
USING DELTA
COMMENT 'Audit log of restriction_processing runs'
""")
