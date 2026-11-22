# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("environment", "")

from generic.config.environment import get_environment

environment = get_environment(
    spark=spark,
    job_environment=dbutils.widgets.get("environment"),
)


# COMMAND ----------

# MAGIC %run ../../src/generic/DataGovernance/CollibraAPI/CollibraAPIClient

# COMMAND ----------

# MAGIC %run ../../src/generic/DataGovernance/CollibraAPI/CollibraDtmViewConfig

# COMMAND ----------

# All combinations to process (previously chosen one at a time via widgets)
iso_country_code = "gsoc"
source_system_names = ["reference", "operational", "transactional"]

domain_selector = {
    "reference": {"gsoc": "11111111-1111-4111-8111-111111111111"},
    "operational": {"gsoc": "22222222-2222-4222-8222-222222222222"},
    "transactional": {"gsoc": "33333333-3333-4333-8333-333333333333"}
}

query_display_length = 50000
catalog_name = f"cdp_data_gsoc_{environment}"
volume_schema = f"meta"

# COMMAND ----------

collibra_client = CollibraAPIClient(
    username=dbutils.secrets.get("kv-gca", "collibra-tech-user"),
    password=dbutils.secrets.get("kv-gca", "collibra-tech-user-passwd"),
)

# COMMAND ----------

import json
import copy

def process_source_system(source_system_name, iso_country_code):
    print(f"\n===== Processing {source_system_name} / {iso_country_code} =====")

    # Fresh copy per run so the domain filter doesn't accumulate across iterations
    config = copy.deepcopy(table_view_config)

    domain_filter = {
        "Field": {
            "name": "Domain_Id",
            "operator": "EQUALS",
            "value": domain_selector[source_system_name][iso_country_code],
        }
    }
    config["TableViewConfig"]["Resources"]["Term"]["Filter"]["AND"].append(domain_filter)
    config["TableViewConfig"]["displayLength"] = query_display_length  # was '==' (no-op)

    payload = json.dumps(config)
    #print(payload)
    data = collibra_client.output_module_export(payload)
    
    # ── write API output to volume ────────────────────────────
    volume_name = f"collibra_config_{source_system_name}_{iso_country_code}"
    file_name = f"{source_system_name}_{iso_country_code}_dtm"
    volume_path = f"/Volumes/{catalog_name}/{volume_schema}/{volume_name}"

    print(f"Writing to volume : {volume_path} and to filename : {file_name}.csv" )

    # ── schema for the volume/Volumes/cdp_data_gsoc_dev/meta/collibra_config_operational_gsoc ──────────────────────────────
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{volume_schema}")

    try:
        dbutils.fs.ls(volume_path)
        print(f"✅ Volume already exists: {volume_name}")
    except Exception:
        print(f"❌ Volume does not exist: {volume_name}")
        try:
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{volume_schema}.{volume_name}")
            print(f"✅ Volume created: {volume_name}")
        except Exception as e:
            print(f"❌ Failed to create volume: {volume_name}. Error: {e}")
            raise

    dbutils.fs.put(
        f"{volume_path}/dtm/{file_name}.csv",
        data,
        overwrite=True,
    )

    # ── schema + per-source view ──────────────────────────────
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.collibra_metadata")

    view_name = f"{catalog_name}.collibra_metadata.vw_{source_system_name}_{iso_country_code}_dtm"
    spark.sql(f"""
        CREATE VIEW IF NOT EXISTS {view_name}
        AS SELECT *
        FROM read_files(
            '{volume_path}/dtm/{file_name}.csv',
            format => 'csv',
            header => true,
            delimiter => ';',
            mode => 'FAILFAST'
        )
    """)


results = {}
for ssn in source_system_names:
    try:
        process_source_system(ssn, iso_country_code)
        results[ssn] = "SUCCESS"
    except Exception as e:
        results[ssn] = f"FAILED: {e}"
        print(f"❌ {ssn} failed: {e}")

print("\nSummary:", results)

failed = {k: v for k, v in results.items() if v != "SUCCESS"}
if failed:
    raise RuntimeError(f"Some source systems failed: {failed}")

# COMMAND ----------

# Union view across all source systems (runs once, after both are loaded)
select_template = """
    Select Distinct concat(
            regexp_substr(Data_Entity_Name,'[^>]+(?=>[^>]*$)'),
            '.',
            regexp_substr(Data_Entity_Name,'[^>]+$')
        ) as TableName,
        Data_Attribute_Name,Treatment_In_Transit,Treatment_At_Rest,
        Cross_Border_Data_Treatment,Asset_Type
    from {full_view_name}
    where Data_Attribute_Name Not IN ('_bronze_ingestion_timestamp','_rescued_data')
"""

source_views = [f"vw_{s}_{iso_country_code}_dtm" for s in source_system_names]

valid_selects = []
for v in source_views:
    full_view_name = f"{catalog_name}.collibra_metadata.{v}"
    if spark.catalog.tableExists(full_view_name):
        valid_selects.append(select_template.format(full_view_name=full_view_name))
    else:
        print(f"Skipping missing view: {full_view_name}")

if valid_selects:
    union_sql = " UNION ".join(valid_selects)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog_name}.collibra_metadata.vw_gsoc_dtm AS
        {union_sql}
    """)
else:
    print("No source views found — skipping view creation.")

# COMMAND ----------

# This cell identifies Silver-layer columns that are marked for pseudonymization
# (per Collibra governance rules) and whose current data type is numeric.
# It then alters those columns from their numeric type to STRING so that
# pseudonymized (hashed/tokenized) values can be stored correctly => overwrite with overwriteSchema option (ALTER COLUMMN NOT POSSIBLE)

from collections import defaultdict
from pyspark.sql import functions as F
from pyspark.sql.types import (
IntegerType,
LongType,
ShortType,
ByteType,
FloatType,
DoubleType,
DecimalType,
)

NUMERIC_TYPES = (
IntegerType,
LongType,
ShortType,
ByteType,
FloatType,
DoubleType,
DecimalType,
)

# Governance rules
governance_df = spark.sql(f"""
SELECT
    concat('{catalog_name}.', TableName) AS source_bronze_table,
    Data_Attribute_Name AS column_name
FROM {catalog_name}.collibra_metadata.vw_gsoc_dtm
WHERE LOWER(Treatment_At_Rest) LIKE '%pseudonymize%'
""")

# Bronze -> Silver mapping
silver_config_df = spark.sql(f"""
SELECT DISTINCT
    source_bronze_table,
    target_silver_table
FROM {catalog_name}.control_gsoc.silver_config
""")

# Resolve impacted Silver tables
impacted_df = (
    governance_df
    .join(
        silver_config_df,
        on="source_bronze_table",
        how="inner"
    )
    .select("target_silver_table", "column_name")
    .distinct()
)

# Group columns by Silver table
columns_by_table = defaultdict(list)

for row in impacted_df.collect():
    columns_by_table[row["target_silver_table"]].append(
        row["column_name"]
    )

for silver_table, governance_columns in columns_by_table.items():

    try:
        df = spark.table(silver_table)

        columns_to_convert = []

        for column_name in governance_columns:

            field = next(
                (f for f in df.schema.fields if f.name.lower() == column_name.lower()),
                None
            )

            if field is None:
                print(f"  Column '{column_name}' not found. Skipping.")
                continue

            if isinstance(field.dataType, NUMERIC_TYPES):
                columns_to_convert.append(
                    (column_name, str(field.dataType))
                )
            

        if not columns_to_convert:            
            continue

        print(f"\nProcessing table: {silver_table}")

        for column_name, current_type in columns_to_convert:
            print(
                f"    - {column_name}: "
                f"{current_type} -> STRING"
            )

            df = df.withColumn(
                column_name,
                F.col(column_name).cast("string")
            )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(silver_table)
        )
        print(
            f"  Successfully updated "
            f"{len(columns_to_convert)} column(s)."
        )

    except Exception as e:
        print(
            f"  Error while processing "
            f"{silver_table}: {e}"
        )
