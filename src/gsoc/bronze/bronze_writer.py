from typing import Any, Dict, List, Optional, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


## Strict Bronze writer for SOC Delta tables.
# Assumptions:
# - Target Bronze tables always exist before ingestion starts.
# - Target table schema is the contract.
# - SSAE API may omit fields when they are empty for all returned records.
# - Missing target columns are added as NULL.    
# - Operational and transactional tables use strict schema validation.
# - Setup full-load tables may optionally allow additive schema evolution.
# - Existing column types are never changed automatically.
# - Existing columns are never removed automatically.
# - No merge/upsert logic is performed in Bronze.

class BronzeWriter:

    ## Initialize the Bronze writer.
    def __init__(
        self,
        spark: SparkSession,
        bronze_run_id_column: str = "bronze_run_id",
        ingested_at_column: str = "ingested_at",
        logger: Optional[Any] = None,
    ) -> None:
        self.spark = spark
        self.bronze_run_id_column = bronze_run_id_column
        self.ingested_at_column = ingested_at_column
        self.logger = logger

    
    ## Write records into a Bronze Delta table.
    #- mode: Write mode. Supported values: - "append": (default) / "overwrite": full refresh, typically for setup tables
    # allow_new_columns:- False: unexpected source columns fail the write. / True: unexpected source columns are added to the target table.
    def write(
        self,
        data: Union[DataFrame, List[Dict[str, Any]]],
        table_name: str,
        bronze_run_id: str,
        mode: str = "append",
        allow_new_columns: bool = False,
    ) -> Dict[str, Any]:
        
        if mode not in ("append", "overwrite"):
            raise ValueError("mode must be either 'append' or 'overwrite'")

        if allow_new_columns and mode != "overwrite":
            allow_new_columns = False  # Force allow_new_columns to False for append mode

        if not self.spark.catalog.tableExists(table_name):
            raise ValueError(f"Target Bronze table does not exist: {table_name}")
        
        records_count = len(data) if isinstance(data, list) else None
        df = self._to_dataframe(data)
        
        if self._is_empty_dataframe(df):
            self._log("info", f"No records to write into {table_name}")
            return {
                "status": "SUCCESS",
                "table_name": table_name,
                "mode": mode,
                "bronze_run_id": bronze_run_id,
                "records_written": 0,
                "columns_added": []
            }

        df = self._add_technical_columns(df, bronze_run_id)

        added_columns: List[str] = []
        if allow_new_columns:
            
            added_columns = self._add_new_columns_to_target(
                df=df,
                table_name=table_name,
            )

        df = self._align_to_target_schema_strict(
            df=df,
            table_name=table_name,
        )

        writer = (
            df.write
            .format("delta")
            .mode(mode)
        )
            
        if mode == "overwrite":
            # The target schema has already been managed explicitly.         
            writer = writer.option("overwriteSchema", "false")
        
        writer.saveAsTable(table_name)

        self._log(
            "info",
            f"Bronze write completed. table={table_name}, mode={mode}, records={records_count}, columns_added={added_columns}",
        )

        return {
            "status": "SUCCESS",
            "table_name": table_name,
            "mode": mode,
            "bronze_run_id": bronze_run_id,
            "records_written": records_count if records_count is not None else -1,
            "columns_added": added_columns
        }

    ##  Convert input data into a Spark DataFrame.
    def _to_dataframe(
        self,
        data: Union[DataFrame, List[Dict[str, Any]]],
    ) -> DataFrame:
        
        if isinstance(data, DataFrame):
            return data

        if isinstance(data, list):
            if len(data) == 0:
                return self.spark.createDataFrame([], StructType([]))
            return self.spark.createDataFrame(data)

        raise TypeError("data must be a Spark DataFrame or a list of dictionaries")

    
    ##  Add minimal technical Bronze columns. bronze_run_id + ingested_at
    def _add_technical_columns(
        self,
        df: DataFrame,
        bronze_run_id: str,
    ) -> DataFrame:
        
        return (
            df
            .withColumn(self.bronze_run_id_column, F.lit(bronze_run_id))
            .withColumn(self.ingested_at_column, F.current_timestamp())
        )


    ## Add new source columns to the target table.
    # This method performs additive schema evolution only.
    # Rules:
    # - Existing columns are not changed.
    # - Existing columns are not removed.
    # - New columns are added as nullable Delta columns.
    def _add_new_columns_to_target(
        self,
        df: DataFrame,
        table_name: str,
    ) -> List[str]:
        target_schema = self.spark.table(table_name).schema

        target_columns_lower = {
            field.name.lower()
            for field in target_schema.fields
        }

        new_fields = [
            field
            for field in df.schema.fields
            if field.name.lower() not in target_columns_lower
        ]

        if not new_fields:
            return []

        column_definitions = ", ".join(
            [
                f"{field.name} {field.dataType.simpleString()}"
                for field in new_fields
            ]
        )

        self.spark.sql(
            f"""
            ALTER TABLE {table_name}
            ADD COLUMNS ({column_definitions})
            """
        )

        added_columns = [
            field.name
            for field in new_fields
        ]

        self._log(
            "warning",
            f"New columns automatically added to setup Bronze table. "
            f"table={table_name}, columns={added_columns}. "
            "The project DDL should be updated accordingly.",
        )

        return added_columns



    ## Align source DataFrame to target table schema.
    #- Existing columns are cast to target type.
    #- Missing target columns are added as NULL and cast to target type.
    #- Unexpected source columns fail the write.
    def _align_to_target_schema_strict(
        self,
        df: DataFrame,
        table_name: str,
    ) -> DataFrame:
        
        target_df = self.spark.table(table_name)
        target_schema = target_df.schema
    
        target_columns = [field.name for field in target_schema.fields]
        target_columns_lower = [col.lower() for col in target_columns]
        source_columns = df.columns
        source_columns_lower = [col.lower() for col in source_columns]

        # Map lower-case source columns to their original names
        source_col_map = {col.lower(): col for col in source_columns}

        unexpected_columns = [
            col_name
            for col_name in source_columns
            if col_name.lower() not in target_columns_lower
        ]

        if unexpected_columns:
            raise ValueError(
                f"Unexpected columns received for target table {table_name}: "
                f"{unexpected_columns}. "
                "Target schema is contractual. Please update the table schema explicitly "
                "before ingesting these fields."
            )

        for field in target_schema.fields:
            field_lower = field.name.lower()
            if field_lower not in source_col_map:
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
            else:
                df = df.withColumn(field.name, F.col(source_col_map[field_lower]).cast(field.dataType))

        return df.select(*target_columns)

    ##  Check whether a DataFrame is empty
    def _is_empty_dataframe(self, df: DataFrame) -> bool:
        
        return len(df.take(1)) == 0

    ##  Log a message if a logger is configured
    def _log(self, level: str, message: str) -> None:
        
        if self.logger is None:
            return

        log_method = getattr(self.logger, level, None)

        if callable(log_method):
            log_method(message)
