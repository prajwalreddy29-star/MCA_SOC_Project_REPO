# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ../_bootstrap

# COMMAND ----------

# Imports
dbutils.widgets.text("environment", "")

from generic.config.environment import get_environment
from generic.common.run_context import RunContext
from generic.common.control_tables import ControlTableHelper
from gsoc.silver.silver_ingestion_service import SilverIngestionService
from gsoc.bronze.bronze_reader import BronzeReader
from gsoc.silver.silver_writer import SilverWriter

# COMMAND ----------

# Control tables
environment = get_environment(
    spark=spark,
    job_environment=dbutils.widgets.get("environment"),
)
catalog = f"cdp_data_gsoc_{environment}"
schema = "control_gsoc"


CONTROL_ENDPOINT_CONFIG_TABLE = f"{catalog}.{schema}.endpoint_config"
CONTROL_STATE_TABLE = f"{catalog}.{schema}.ingestion_state"
CONTROL_SILVER_CONFIG_TABLE = f"{catalog}.{schema}.silver_config"
CONTROL_SILVER_PROCESSING_QUEUE = f"{catalog}.{schema}.silver_processing_queue"
CONTROL_SILVER_RUN_LOG_TABLE = f"{catalog}.{schema}.silver_run_log"
COLLIBRA_DEFINITION_TABLE = f"{catalog}.collibra_metadata.vw_gsoc_dtm"

AES_KEY = dbutils.secrets.get(scope="kv-gca", key="cdp_encryption_key")

# COMMAND ----------

# Initialize dependencies


control_helper = ControlTableHelper(
    spark=spark,
    config_table=CONTROL_ENDPOINT_CONFIG_TABLE,
    run_log_table=CONTROL_SILVER_RUN_LOG_TABLE,
    state_table=CONTROL_STATE_TABLE,
    silver_config_table=CONTROL_SILVER_CONFIG_TABLE,
    silver_processing_queue_table=CONTROL_SILVER_PROCESSING_QUEUE
)

context = RunContext(
        layer="silver",
        load_group="setup",
        load_phase="",
        dbutils_obj=dbutils
        )

silver_writer = SilverWriter(
    spark=spark
)

bronze_reader = BronzeReader(
    spark=spark,
    aes_key=AES_KEY,
    collibra_definition_table=COLLIBRA_DEFINITION_TABLE
)


silver_service = SilverIngestionService(   
    control_helper=control_helper,
    bronze_reader=bronze_reader,
    silver_writer=silver_writer,
    max_workers=8    
)


# COMMAND ----------

# Run setup ingestion

summary = silver_service.run(
    context=context,
    ingestion_mode = "full"
)

display(summary)

# COMMAND ----------

# Fail the Databricks job if at least one endpoint failed

if summary["failure_count"] > 0:
    raise Exception(f"Silver completed with failures: {summary}")
