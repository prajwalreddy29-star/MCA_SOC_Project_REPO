# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

# Imports
dbutils.widgets.text("environment", "")

from generic.config.environment import get_environment
# ==========================================
# SSAE BRONZE TABLE CREATION SETUP TABLES
# ==========================================

# -------------------------------
# PARAMETERS
# -------------------------------
environment = get_environment(
    spark=spark,
    job_environment=dbutils.widgets.get("environment"),
)
catalog = f"cdp_data_gsoc_{environment}"
bronze_schema = "bronze_gsoc_reference"
silver_schema = "silver_gsoc_reference"


#spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

def create(table_name, structure):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.{table_name}
    (
        {structure},
        bronze_run_id STRING,
        ingested_at TIMESTAMP
    ) USING DELTA
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.{table_name}
    (
        {structure},
        silver_run_id STRING,
        silver_created_at TIMESTAMP,
        silver_updated_at TIMESTAMP
    ) USING DELTA
    """)

# -------------------------
# AGENCY
# -------------------------
create("agency", """
    agency_no INT,
    agency_name STRING,
    agency_addr1 STRING,
    agency_addr2 STRING,
    agencytype_id STRING,
    active_flag STRING,
    attention STRING,
    city_name STRING,
    state_id STRING,
    zip_code STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# AGENCY TYPE
# -------------------------
create("agencytype", """
    agencytype_id STRING,
    descr STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# BRANCH
# -------------------------
create("branch", """
    branch_no INT,
    branch_name STRING,
    udf1 STRING,
    udf2 STRING,
    udf3 STRING,
    udf4 STRING,
    udf5 STRING,
    udf6 STRING,
    udf7 STRING,
    udf8 STRING,
    udf9 STRING,
    udf10 STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# BRANDING
# -------------------------
create("branding", """
    branding_id STRING,
    company_name STRING,
    city_name STRING,
    email_address STRING,
    phone_number STRING,
    address1 STRING,
    address2 STRING,
    state_id STRING,
    zip_code STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# CALL DISPOSITION
# -------------------------
create("calldisposition", """
    calldisp_id STRING,
    descr STRING,
    event_id STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# CS PARTITION
# -------------------------
create("cspartition", """
    cspart_no INT,
    descr STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# DST
# -------------------------
create("daylightsavingstime", """
    dst_no TINYINT,
    descr STRING,
    change_date TIMESTAMP,
    change_user INT
""")


# -------------------------
# EQUIPMENT TYPE
# -------------------------
create("equipmenttype", """
    equiptype_id STRING,
    descr STRING,
    twoway_device_id STRING,
    usage STRING,
    active_flag STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# EVENT
# -------------------------
create("event", """
    event_id STRING,
    descr STRING,
    event_class STRING,
    eventrpt_id STRING,
    full_clear_flag STRING,
    opact_flag STRING,
    active_flag STRING,
    long_descr STRING,
    servtype_id STRING,
    signotype_id STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# EVENT REPORT CODE
# -------------------------
create("eventreportcode", """
    eventrpt_id STRING,
    descr STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# LANGUAGE
# -------------------------
create("language", """
    lang_id STRING,
    descr STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# OOS CATEGORY
# -------------------------
create("ooscategory", """
    ooscat_id STRING,
    descr STRING,
    active_flag STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# OPERATOR ACTION
# -------------------------
create("operatoractiondisposition", """
    opactdisp_id STRING,
    descr STRING,
    udf1 STRING,
    udf2 STRING,
    udf3 STRING,
    udf4 STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# OPTION
# -------------------------
create("option", """
    option_id STRING,
    descr STRING,
    active_flag STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# PHONE TYPE
# -------------------------
create("phonetype", """
    phonetype_id STRING,
    descr STRING,
    method STRING,
    change_date TIMESTAMP,
    change_user INT
""")


# -------------------------
# SERVICE COMPANY
# -------------------------
create("servicecompany", """
    servco_no INT,
    servco_name STRING,
    city_name STRING,
    country_name STRING,
    state_id STRING,
    attention STRING,
    branding_id STRING,
    rollup_servco_no INT,
    servco_addr1 STRING,
    servco_addr2 STRING,
    zip_code STRING,
    servco_type STRING,
    udf1 STRING,
    udf2 STRING,
    udf3 STRING,
    udf4 STRING,
    active_flag STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# SERVICE TYPE
# -------------------------
create("servicetype", """
    servtype_id STRING,
    descr STRING,
    active_flag STRING,
    slC_type STRING,
    udf1 STRING,
    udf2 STRING,
    udf3 STRING,
    udf4 STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# SITE LOCATION
# -------------------------
create("sitelocation", """
    siteloc_id STRING,
    descr STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# SITE STATUS
# -------------------------
create("sitestatus", """
    sitestat_id STRING,
    descr STRING,
    active_flag STRING,
    change_date TIMESTAMP,
    change_user INT
""")


# -------------------------
# SITE TYPE
# -------------------------
create("sitetype", """
    sitetype_id STRING,
    descr STRING,
    site_kind STRING,
    active_flag STRING,
    udf1 STRING,
    udf2 STRING,
    udf3 STRING,
    udf4 STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# STATE
# -------------------------
create("state", """
    state_id STRING,
    state_name STRING,
    country_name STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# SYSTEM TYPE
# -------------------------
create("systemtype", """
    systype_id STRING,
    descr STRING,
    active_flag STRING,
    udf1 STRING,
    udf2 STRING,
    udf3 STRING,
    udf4 STRING,
    change_date TIMESTAMP,
    change_user INT
""")

# -------------------------
# TIME ZONE
# -------------------------
create("timezone", """
    timezone_no SMALLINT,
    descr STRING,
    offset_hour SMALLINT,
    offset_min SMALLINT,
    change_date TIMESTAMP,
    change_user INT
""")
