# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

# Imports
dbutils.widgets.text("environment", "")

from generic.config.environment import get_environment
from generic.common.run_context import RunContext
from generic.common.control_tables import ControlTableHelper
from gsoc.bronze.bronze_writer import BronzeWriter
from generic.ssae_api.ssae_client import SsaeApiClient
from gsoc.restrictions.restriction_processing import RestrictionProcessingService


# COMMAND ----------

# Control tables
environment = get_environment(
    spark=spark,
    job_environment=dbutils.widgets.get("environment"),
)
catalog = f"cdp_data_gsoc_{environment}"
control_schema = "control_gsoc"
bronze_schema_operational = "bronze_gsoc_operational"
silver_schema_operational = "silver_gsoc_operational"
silver_schema_transactional = "silver_gsoc_transactional"

# Control tables
CONTROL_ENDPOINT_CONFIG_TABLE = f"{catalog}.{control_schema}.endpoint_config"
CONTROL_RUN_LOG_TABLE = f"{catalog}.{control_schema}.ingestion_run_log"
CONTROL_STATE_TABLE = f"{catalog}.{control_schema}.ingestion_state"
CONTROL_SILVER_CONFIG_TABLE = f"{catalog}.{control_schema}.silver_config"
CONTROL_SILVER_PROCESSING_QUEUE = f"{catalog}.{control_schema}.silver_processing_queue"
CONTROL_RESTRICTION_QUEUE = f"{catalog}.{control_schema}.restriction_processing_queue"
CONTROL_RESTRICTION_LOG = f"{catalog}.{control_schema}.restriction_processing_log"

# SSAE API client
SSAE_BASE_URL = dbutils.secrets.get("api_ssae_scope", "api_base_url")
SSAE_ACCESS_KEY = dbutils.secrets.get("api_ssae_scope", "api_access_key")
SSAE_AUTH_KEY = dbutils.secrets.get("api_ssae_scope", "api_auth_key")


client = SsaeApiClient(
    base_url=SSAE_BASE_URL,
    access_key=SSAE_ACCESS_KEY,
    auth_key=SSAE_AUTH_KEY,
    timeout=30,
    max_retries=1,
    debug=False,
)

# Bronze writer & reader
bronze_writer = BronzeWriter(
    spark=spark,
    bronze_run_id_column="bronze_run_id",
    ingested_at_column="ingested_at",
)

# Control table helper
control_helper = ControlTableHelper(
    spark=spark,
    config_table=CONTROL_ENDPOINT_CONFIG_TABLE,
    run_log_table=CONTROL_RUN_LOG_TABLE,
    state_table=CONTROL_STATE_TABLE,
    silver_config_table=CONTROL_SILVER_CONFIG_TABLE,
    silver_processing_queue_table=CONTROL_SILVER_PROCESSING_QUEUE,
    restriction_processing_queue_table=CONTROL_RESTRICTION_QUEUE,
    restriction_log_table=CONTROL_RESTRICTION_LOG,
)

# Restriction config (country-specific table mapping)
restriction_config = {
    "api_country_code": "GC_EU",
    "site_bronze_table": f"{catalog}.{bronze_schema_operational}.site",
    "system_bronze_table": f"{catalog}.{bronze_schema_operational}.system",
    "site_silver_table": f"{catalog}.{silver_schema_operational}.site",
    "system_silver_table": f"{catalog}.{silver_schema_operational}.system",
    "alarm_incident_silver_table": f"{catalog}.{silver_schema_transactional}.alarm_incident",
    "alarm_incident_call_silver_table": f"{catalog}.{silver_schema_transactional}.alarm_incident_call",
    "event_history_silver_table": f"{catalog}.{silver_schema_transactional}.event_history",
}

# Run context
context = RunContext(
    layer="restriction",
    load_group="restriction",
    load_phase="processing",
    dbutils_obj=dbutils,
)

# Service
service = RestrictionProcessingService(
    spark=spark,
    client=client,
    bronze_writer=bronze_writer,
    control_helper=control_helper,
    restriction_config=restriction_config,
    delete_mode="SOFT",
)

summary = service.run(
    context=context,
    restriction_action=None,  # Process all pending actions (INCLUDE + EXCLUDE)
)

display(summary)

if summary["failure_count"] > 0:
    raise Exception(f"Restriction processing completed with failures: {summary}")
