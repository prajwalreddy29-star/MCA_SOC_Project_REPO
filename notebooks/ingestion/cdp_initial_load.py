# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("environment", "")

from generic.config.environment import get_environment
from generic.common.run_context import RunContext
from generic.common.control_tables import ControlTableHelper
from gsoc.bronze.bronze_writer import BronzeWriter
from generic.ssae_api.ssae_client import SsaeApiClient
from gsoc.bronze.ingestion.cdp_initial_ingestion import CdpInitialIngestionService

environment = get_environment(
    spark=spark,
    job_environment=dbutils.widgets.get("environment"),
)
catalog = f"cdp_data_gsoc_{environment}"
schema = "control_gsoc"

CONTROL_ENDPOINT_CONFIG_TABLE = f"{catalog}.{schema}.endpoint_config"
CONTROL_RUN_LOG_TABLE = f"{catalog}.{schema}.ingestion_run_log"
CONTROL_STATE_TABLE = f"{catalog}.{schema}.ingestion_state"
CONTROL_SILVER_CONFIG_TABLE = f"{catalog}.{schema}.silver_config"
CONTROL_SILVER_PROCESSING_QUEUE = f"{catalog}.{schema}.silver_processing_queue"


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

client._get_token()

bronze_writer = BronzeWriter(
    spark=spark,
    bronze_run_id_column="bronze_run_id",
    ingested_at_column="ingested_at",
)

control_helper = ControlTableHelper(
    spark=spark,
    config_table=CONTROL_ENDPOINT_CONFIG_TABLE,
    run_log_table=CONTROL_RUN_LOG_TABLE,
    state_table=CONTROL_STATE_TABLE,
    silver_config_table=CONTROL_SILVER_CONFIG_TABLE,
    silver_processing_queue_table=CONTROL_SILVER_PROCESSING_QUEUE
)

context = RunContext(
    layer = "bronze",
    load_group = "cdp",
    load_phase = "initial",
    dbutils_obj=dbutils
)

service = CdpInitialIngestionService(
    client=client,
    bronze_writer=bronze_writer,
    control_helper=control_helper
)

summary = service.run(
    mode="initial",
    context=context
)

display(summary)

if summary["failure_count"] > 0:
    raise Exception(f"CDP initial ingestion completed with failures: {summary}")
