from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

## Bronze reader for SOC Delta tables.
# Responsibilities:
# - Read Bronze records for one or multiple Bronze run ids.
# - Keep Bronze access logic out of SilverIngestionService.
# - Centralize Bronze technical column names.

class BronzeReader:

    ## Initialize the Bronze reader.
    def __init__(
        self,
        spark: SparkSession,        
        aes_key: str,
        collibra_definition_table: str,
        bronze_run_id_column: str = "bronze_run_id"
    ) -> None:

        self.spark = spark
        self.bronze_run_id_column = bronze_run_id_column
        self.aes_key = aes_key
        self.collibra_definition_table = collibra_definition_table


    ## Read Bronze data for a specific Bronze run id.
    def read_for_run(
        self,
        table_name: str,
        bronze_run_id: str,
    ) -> DataFrame:

        bronze_df = (
            self.spark.table(table_name)
            .filter(F.col(self.bronze_run_id_column) == F.lit(bronze_run_id))
        )
        
        short_table_name = table_name.rsplit(".", 1)[-1] if "." in table_name else table_name
        governance_columns   = self.get_pseudonimized_columns_list(short_table_name)
        if governance_columns:
            bronze_df = self.apply_data_treatment(bronze_df, governance_columns)

        return bronze_df


    ## Read Bronze data for multiple Bronze run ids.
    # def read_for_runs(
    #     self,
    #     table_name: str,
    #     bronze_run_ids: List[str],
    # ) -> DataFrame:

    #     if not bronze_run_ids:
    #         return self.spark.table(table_name).limit(0)

    #     return (
    #         self.spark.table(table_name)
    #         .filter(F.col(self.bronze_run_id_column).isin(bronze_run_ids))
    #     )

    # Reads Pseudonymize columns for the configured entity from the Unity Catalog view.
    # Returns:
    #   dtm_df  : list of Pseudonymize column names    
    # Returns ([], []) if the table is unreachable or entity has no matches.    
    def get_pseudonimized_columns_list(
        self,
        short_table_name: str
    ):
        
        try:
            
            dtm_df = (
                self.spark.table(f"{self.collibra_definition_table}")
                .filter(
                    (F.element_at(F.split(F.lower(F.col("TableName")), F.lit("\\.")), -1) == F.lower(F.lit(short_table_name))) &   
                    (F.col("Asset_Type") == "Data Attribute") &
                    (
                        F.col("Treatment_At_Rest").like("%Pseudonymize%") |
                        F.col("Treatment_At_Rest").like("%Prohibited%")
                    )
                )
                .select(
                    F.element_at(F.split(F.lower(F.col("TableName")), F.lit("\\.")), -1).alias("TableName"),                     
                    F.lower(F.col("Data_Attribute_Name")).alias("Data_Attribute_Name"), 
                    F.col("Treatment_At_Rest"),
                    F.col("Asset_Type")
                )
                .distinct()
            )
            return dtm_df.collect()

        except Exception as e:
            print(f"Could not read DTM view: {e}") 
            return []

    # Apply data governance treatment: 
    # pseudonymize => aes_encrypt
    # prohibited => replaced by 0
    # Returns the DataFrame with the apply command on concerned columns
    def apply_data_treatment(
        self,
        bronze_df: DataFrame, 
        pseudonimized_columns_list: list
    ) -> DataFrame:
        
        for column in pseudonimized_columns_list:
            attribute_name = column.Data_Attribute_Name
            treatment_type = column.Treatment_At_Rest.strip().lower() if column.Treatment_At_Rest else ""

            if attribute_name in bronze_df.columns:
                if "pseudonymize" in treatment_type:
                    bronze_df = bronze_df.withColumn(
                        attribute_name,
                        F.base64(
                            F.aes_encrypt(
                                input=F.col(attribute_name).cast("string"),
                                key=F.lit(self.aes_key),
                                mode=F.lit("ECB"),
                                padding=F.lit("PKCS")
                            )
                        )
                    )
                elif "prohibited" in treatment_type:
                    print(f"Replacing prohibited values with 0 for column: {attribute_name}")
                    bronze_df = bronze_df.withColumn(attribute_name, F.lit(0).cast("int"))
            else:
                print(f"Column {attribute_name} not found in bronze_df, skipping transformation.")   # FIX: self.logger.warning → print
        return bronze_df    