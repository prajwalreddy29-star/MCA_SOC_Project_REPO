# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

# Imports
dbutils.widgets.text("environment", "")

from generic.config.environment import get_environment
# ==========================================
# SSAE BRONZE TABLE CREATION OPERATIONAL TABLES
# ==========================================

# -------------------------------
# PARAMETERS
# -------------------------------
environment = get_environment(
    spark=spark,
    job_environment=dbutils.widgets.get("environment"),
)
catalog = f"cdp_data_gsoc_{environment}"
bronze_schema = "bronze_gsoc_operational"
silver_schema = "silver_gsoc_operational"


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

change_fields = """
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
# SITE
# -------------------------

site_structure = """
    site_no INT,
    attention STRING,
    branch_no INT,
    branding_id STRING,
    change_date TIMESTAMP,
    change_type STRING,
    city_name STRING,
    country_name STRING,
    county_name STRING,
    latitude DECIMAL(12,8),
    longitude DECIMAL(12,8),
    site_name STRING,
    sitestat_id STRING,
    sitetype_id STRING,
    state_id STRING,
    corpacct_servco_no INT,
    cross_street STRING,
    cspart_no INT,
    dst_no TINYINT,
    ext1 STRING,
    ext2 STRING,
    install_servco_no INT,
    lang_id STRING,
    orig_install_date TIMESTAMP,
    phone1 STRING,
    phone2 STRING,
    servco_no INT,
    site_addr1 STRING,
    site_addr2 STRING,
    siteloc_id STRING,
    udf1 STRING,
    udf2 STRING,
    udf3 STRING,
    udf4 STRING,
    utc_date TIMESTAMP,
    timezone_no SMALLINT,
    zip_code STRING
"""

create_bronze("site", site_structure)
create_bronze("site_change", site_structure + "," + change_fields )
create_bronze("site_delete", site_structure + "," + change_fields )

create_silver("site", site_structure + "," + silver_fields)  #Silver : creation of site only


# -------------------------
# SYSTEM
# -------------------------
system_structure = """
    system_no INT,
    site_no INT,
    cs_no	STRING,
    descr STRING,
    equiptype_id STRING,
    systype_id STRING,
    install_date TIMESTAMP,
    active_date TIMESTAMP,
    inactive_date TIMESTAMP,
    servco_no INT,
    change_date	TIMESTAMP,
    change_type	STRING,
    dispatch_comment	STRING,
    install_servco_no	INT,
    oos_start_date	TIMESTAMP,
    oos_zone_list	STRING,
    ooscat_id	STRING,
    restricted_flag	STRING,
    udf1 STRING,
    udf2 STRING,
    udf3 STRING,
    udf4 STRING,
    utc_date TIMESTAMP
"""

create_bronze("system", system_structure)
create_bronze("system_change", system_structure + "," + change_fields )
create_bronze("system_delete", system_structure + "," + change_fields )

create_silver("system", system_structure + "," + silver_fields)  #Silver : creation of system only

# -------------------------
# RESTRICTION / INCLUSION
# -------------------------
create_bronze("restriction_inclusion", """
    site_no INT
""")

# -------------------------
# RESTRICTION / EXCLUSION
# -------------------------
create_bronze("restriction_exclusion", """
    site_no INT
""")


# -------------------------
# RESTRICTION / EXCLUSION
# Difference in "changes" fields
# -------------------------
create_bronze("restriction_change", """
    site_no INT,
    change_date TIMESTAMP,
    tran_date TIMESTAMP,
    tran_seqno DECIMAL(18,0),
    tran_server STRING,
    excluded BOOLEAN
""")


# TODO Silver for restriction : to analyze
