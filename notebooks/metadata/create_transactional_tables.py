# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

# Imports
dbutils.widgets.text("environment", "")

from generic.config.environment import get_environment
# ==========================================
# SSAE BRONZE TABLE CREATION TRANSACTIONAL TABLES
# ==========================================

# -------------------------------
# PARAMETERS
# -------------------------------
environment = get_environment(
    spark=spark,
    job_environment=dbutils.widgets.get("environment"),
)
catalog = f"cdp_data_gsoc_{environment}"
bronze_schema = "bronze_gsoc_transactional"
silver_schema = "silver_gsoc_transactional"


#spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

def create_bronze(table_name, structure):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.{table_name}
    (
        {structure},
        bronze_run_id STRING,
        ingested_at TIMESTAMP
    ) USING DELTA
    """)


def create_silver(table_name, structure):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.{table_name}
    (
        {structure}
    ) USING DELTA
    """)        

change_structure = """
    tran_date TIMESTAMP,
    tran_seqno DECIMAL(18,0),
    tran_server STRING,
    tran_type STRING
"""

silver_fields = """
    silver_run_id STRING,
    silver_created_at TIMESTAMP,
    silver_updated_at TIMESTAMP,
    silver_is_deleted BOOLEAN,
    silver_deleted_at TIMESTAMP
"""

# -------------------------
# ALARM INCIDENT
# -------------------------
alarm_incident_structure = """
    agency_no INT,
    agencytype_id STRING,
    alarm_local_date TIMESTAMP,
    alarm_server_date TIMESTAMP,
    alarm_zone_id STRING,
    alarminc_no DECIMAL(18,0),
    confirmed_alarm_flag STRING,
    confirmed_alarm_group STRING,
    event_id STRING,
    first_access_local_date TIMESTAMP,
    first_access_server_date TIMESTAMP,
    first_emp_no INT,
    full_clear_emp_no INT,
    full_clear_local_date TIMESTAMP,
    full_clear_opactdisp_id STRING,
    full_clear_server_date TIMESTAMP,
    priority SMALLINT,
    servco_no INT,
    servtype_id STRING,
    status STRING,
    system_no INT
"""
create_bronze("alarm_incident", alarm_incident_structure)
create_bronze("alarm_incident_change", alarm_incident_structure + "," + change_structure )

create_silver("alarm_incident", alarm_incident_structure + "," + silver_fields)  #Silver : creation of alarm_incident only


# -------------------------
# ALARM INCIDENT CALL
# -------------------------
alarm_incident_call_structure = """
    agency_no INT,
    agencytype_id STRING,
    alarminc_no DECIMAL(18,0),
    calldisp_id STRING,
    calllist_no INT,
    calllist_seqno SMALLINT,
    calllist_type STRING,
    change_date TIMESTAMP,
    contact_no INT,
    extension STRING,
    phonetype_id STRING,
    seqno SMALLINT
"""

create_bronze("alarm_incident_call", alarm_incident_call_structure)
create_bronze("alarm_incident_call_change", alarm_incident_call_structure + "," + change_structure)

create_silver("alarm_incident_call", alarm_incident_call_structure + "," + silver_fields)  #Silver : creation of alarm_incident_call only

# -------------------------
# EVENT HISTORY
# -------------------------
event_history_structure = """
    additional_info STRING,
    alarminc_call_seqno INT,
    alarminc_no DECIMAL(18,0),
    area STRING,
    comment STRING,
    emp_no INT,
    employee_initials STRING,
    equiploc_id STRING,
    event_class STRING,
    event_date TIMESTAMP,
    event_descr STRING,
    event_id STRING,
    eventrpt_id STRING,
    full_clear_flag STRING,
    opactdisp_descr STRING,
    opactdisp_id STRING,
    phone STRING,
    seqno DECIMAL(18,0),
    server_id STRING,
    system_no INT,
    test_seqno INT,
    user_id STRING,
    user_name STRING,
    zone_comment STRING,
    zone_id STRING
"""

create_bronze("event_history", event_history_structure)

create_silver("event_history", event_history_structure + "," + silver_fields)   #Silver : creation of event_history only
