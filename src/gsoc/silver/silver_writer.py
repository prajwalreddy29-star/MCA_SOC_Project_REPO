from typing import Any, Dict, List, Optional, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
from delta.tables import DeltaTable


## Strict Silver writer for SOC Delta tables.
# Assumptions:
# - Target Silver tables always exist before processing starts.
# - Target table schema is the contract.
# - Bronze technical columns are not propagated to Silver.
# - Missing target columns are added as NULL.
# - No automatic schema evolution is allowed.
# - Silver supports multiple write patterns:
#   - full_replace: for setup/reference tables.
#   - append: for append-only entities such as EventHistory.
#   - merge_scd1: for mutable entities updated from change endpoints.
#   - soft_delete: for delete endpoints converted to logical deletes.

class SilverWriter:

    ## Initialize the Silver writer.
    def __init__(
        self,
        spark: SparkSession,
        silver_run_id_column: str = "silver_run_id",
        silver_created_at_column: str = "silver_created_at",
        silver_updated_at_column: str = "silver_updated_at",
        silver_is_deleted_column: str = "silver_is_deleted",
        silver_deleted_at_column: str = "silver_deleted_at",
        bronze_run_id_column: str = "bronze_run_id",
        bronze_ingested_at_column: str = "ingested_at",
        tran_fields: Optional[List[str]] = ["tran_date","tran_seqno","tran_server","tran_type"],
        logger: Optional[Any] = None,
    ) -> None:

        self.spark = spark
        self.silver_run_id_column = silver_run_id_column
        self.silver_created_at_column = silver_created_at_column
        self.silver_updated_at_column = silver_updated_at_column
        self.silver_is_deleted_column = silver_is_deleted_column
        self.silver_deleted_at_column = silver_deleted_at_column
        self.bronze_run_id_column = bronze_run_id_column
        self.bronze_ingested_at_column = bronze_ingested_at_column
        self.tran_fields = tran_fields or []
        self.logger = logger


    ## Fully replace a Silver table.
    # Typical use case:
    # - Setup/reference tables loaded as full snapshots.
    def full_replace(
        self,
        data: Union[DataFrame, List[Dict[str, Any]]],
        table_name: str,
        silver_run_id: str,
        allow_new_columns: bool = False,
    ) -> Dict[str, Any]:

        self._validate_target_table(table_name)
        df = self._to_dataframe(data)

        if self._is_empty_dataframe(df):
            self._log("info", f"No records to replace into {table_name}")
            return self._result(
                status="SUCCESS",
                table_name=table_name,
                mode="full_replace",
                silver_run_id=silver_run_id,
                records_written=0,
            )

        records_count = self._safe_count(df)

        df = self._prepare_silver_dataframe(
            df=df,
            table_name=table_name,
            silver_run_id=silver_run_id,
            mode="insert",
            category="full_replace",
            allow_new_columns=allow_new_columns,
        )
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(table_name)
        )

        self._log(
            "info",
            f"Silver full_replace completed. table={table_name}, records={records_count}",
        )

        return self._result(
            status="SUCCESS",
            table_name=table_name,
            mode="full_replace",
            silver_run_id=silver_run_id,
            records_written=records_count,
        )


    ## Append records into a Silver table.
    # Typical use case:
    # - Append-only entities such as EventHistory.
    def append(
        self,
        data: Union[DataFrame, List[Dict[str, Any]]],
        table_name: str,
        silver_run_id: str,
        key_columns=None,
        max_silver_run_id=None, 
    ) -> Dict[str, Any]:

        self._validate_target_table(table_name)

        df = self._to_dataframe(data)

        # 1 - Prepare Silver DataFrame
        df = self._prepare_silver_dataframe(
            df=df,
            table_name=table_name,
            silver_run_id=silver_run_id,
            mode="insert",
            category="append",
        )
        
        # 2 - Deduplicate against last successful Silver run      
        if key_columns and max_silver_run_id:

            df = self._deduplicate_against_last_success_run(
                source_df=df,
                table_name=table_name,
                key_columns=key_columns,
                max_silver_run_id=max_silver_run_id,
            )
        
        if self._is_empty_dataframe(df):
            self._log("info", f"No records to append into {table_name}")
            return self._result(
                status="SUCCESS",
                table_name=table_name,
                mode="append",
                silver_run_id=silver_run_id,
                records_written=0,
            )

        # 3 - Write to Delta
        records_count = self._safe_count(df)
        
        (
            df.write
            .format("delta")
            .mode("append")
            .option("overwriteSchema", "false")
            .saveAsTable(table_name)
        )

        self._log(
            "info",
            f"Silver append completed. table={table_name}, records={records_count}",
        )

        return self._result(
            status="SUCCESS",
            table_name=table_name,
            mode="append",
            silver_run_id=silver_run_id,
            records_written=records_count,
        )


    ## Merge records into a Silver table using SCD Type 1 semantics.
    # Typical use case:
    # - Mutable entities updated from change endpoints.
    # - Existing rows are updated.
    # - New rows are inserted.
    # - silver_created_at is preserved on update.
    # - silver_updated_at is refreshed on update.
    def merge_scd1(
        self,
        data: Union[DataFrame, List[Dict[str, Any]]],
        table_name: str,
        silver_run_id: str,
        key_columns: List[str],
    ) -> Dict[str, Any]:

        self._validate_target_table(table_name)

        if not key_columns:
            raise ValueError("key_columns must not be empty for merge_scd1")

        df = self._to_dataframe(data)

        if self._is_empty_dataframe(df):
            self._log("info", f"No records to merge into {table_name}")
            return self._result(
                status="SUCCESS",
                table_name=table_name,
                mode="merge_scd1",
                silver_run_id=silver_run_id,
                records_written=0,
            )

        records_count = self._safe_count(df)

        df = self._prepare_silver_dataframe(
            df=df,
            table_name=table_name,
            silver_run_id=silver_run_id,
            mode="upsert",
            category="merge_scd1",
        )

        target = DeltaTable.forName(self.spark, table_name)
        target_columns = self.spark.table(table_name).columns

        merge_condition = " AND ".join(
            [f"t.{col} = s.{col}" for col in key_columns]
        )

        update_set = {
            col: f"s.{col}"
            for col in target_columns
            if col not in key_columns
            and col != self.silver_created_at_column
        }

        insert_values = {
            col: f"s.{col}"
            for col in target_columns
        }

        (
            target.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )

        self._log(
            "info",
            f"Silver merge_scd1 completed. table={table_name}, records={records_count}",
        )

        return self._result(
            status="SUCCESS",
            table_name=table_name,
            mode="merge_scd1",
            silver_run_id=silver_run_id,
            records_written=records_count,
        )


    ## Apply CDC changes (Add / Change) into a Silver table.
    # Rules:
    # - 'A' (Add): insert only if the PK does not exist
    # - 'C' (Change):
    # - update if the row exists
    # - insert if the row does not exist => !! We can receive a Change "C" without an Addition "A" before, It's normal.
    def merge_changes(
        self,
        data: Union[DataFrame, List[Dict[str, Any]]],
        table_name: str,
        silver_run_id: str,
        key_columns: List[str],
        tran_type_column: str = "tran_type",
    ) -> Dict[str, Any]:

        self._validate_target_table(table_name)

        if not key_columns:
            raise ValueError("key_columns must not be empty for merge_changes")

        df = self._to_dataframe(data)

        if self._is_empty_dataframe(df):
            return self._result(
                status="SUCCESS",
                table_name=table_name,
                mode="merge_changes",
                silver_run_id=silver_run_id,
                records_written=0
            )

        # Step 1 - Keep latest change if we get multiple add/changes for the same key_columns
        df = self._keep_latest_change(
                df=df,
                key_columns=key_columns,
            )

        # Step 2 - Split A / C
        df_insert = df.filter(F.col(tran_type_column) == F.lit("A"))
        df_update = df.filter(F.col(tran_type_column) == F.lit("C"))

        # Step 3 - Prepare Silver DF        
        df_insert = self._prepare_silver_dataframe(
            df=df_insert,
            table_name=table_name,
            silver_run_id=silver_run_id,
            mode="insert", 
            category="merge_changes"
        )

        df_update = self._prepare_silver_dataframe(
            df=df_update,
            table_name=table_name,
            silver_run_id=silver_run_id,
            mode="upsert", 
            category="merge_changes"
        )

        target = DeltaTable.forName(self.spark, table_name)

        # Step 4 - INSERT ONLY IF NOT EXISTS
        if not self._is_empty_dataframe(df_insert):

            merge_condition = " AND ".join(
                [f"t.{col} = s.{col}" for col in key_columns]
            )

            (
                target.alias("t")
                .merge(df_insert.alias("s"), merge_condition)
                .whenNotMatchedInsertAll()
                .execute()
            )

        # Step 5 - UPDATE IF EXISTS
        if not self._is_empty_dataframe(df_update):

            merge_condition = " AND ".join(
                [f"t.{col} = s.{col}" for col in key_columns]
            )

            target_columns = self.spark.table(table_name).columns

            update_set = {
                col: f"s.{col}"
                for col in target_columns
                if col not in key_columns and col != "silver_created_at"
            }

            (
                target.alias("t")
                .merge(df_update.alias("s"), merge_condition)
                .whenMatchedUpdate(set=update_set)
                .whenNotMatchedInsertAll()
                .execute()
            )

        # Step 6 - Return result
        records_count = df.count()

        return self._result(
            "SUCCESS",
            table_name,
            "merge_changes",
            silver_run_id,
            records_count,
        )

    ## Keep the latest changes for each key_columns
    # To prevent error in MERGE
    # Use the tran_date as the field to order the changes and take the latest
    def _keep_latest_change(
        self,
        df: DataFrame,
        key_columns: List[str],
    ) -> DataFrame:

        ordering = [
            F.col("tran_date").desc_nulls_last()
        ]

        window = (
            Window
            .partitionBy(*key_columns)
            .orderBy(*ordering)
        )

        return (
            df
            .withColumn(
                "_silver_change_rank",
                F.row_number().over(window),
            )
            .filter(F.col("_silver_change_rank") == 1)
            .drop("_silver_change_rank")
        )


    ## Apply logical delete in a Silver table.
    # Typical use case:
    # - Delete endpoints such as site deletes or system deletes.
    # - Records are not physically deleted.
    # - silver_is_deleted is set to true.
    # - silver_deleted_at is set to current timestamp.
    def soft_delete(
        self,
        data: Union[DataFrame, List[Dict[str, Any]]],
        table_name: str,
        silver_run_id: str,
        key_columns: List[str],
    ) -> Dict[str, Any]:

        self._validate_target_table(table_name)

        if not key_columns:
            raise ValueError("key_columns must not be empty for soft_delete")

        df = self._to_dataframe(data)

        if self._is_empty_dataframe(df):
            self._log("info", f"No records to soft delete in {table_name}")
            return self._result(
                status="SUCCESS",
                table_name=table_name,
                mode="soft_delete",
                silver_run_id=silver_run_id,
                records_written=0,
            )

        records_count = self._safe_count(df)

        df = self._remove_bronze_technical_columns(df)

        target = DeltaTable.forName(self.spark, table_name)

        merge_condition = " AND ".join(
            [f"t.{col} = s.{col}" for col in key_columns]
        )

        update_set = {
            self.silver_run_id_column: f"'{silver_run_id}'",
            self.silver_updated_at_column: "current_timestamp()",
            self.silver_is_deleted_column: "true",
            self.silver_deleted_at_column: "current_timestamp()",
        }

        (
            target.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .execute()
        )

        self._log(
            "info",
            f"Silver soft_delete completed. table={table_name}, records={records_count}",
        )

        return self._result(
            status="SUCCESS",
            table_name=table_name,
            mode="soft_delete",
            silver_run_id=silver_run_id,
            records_written=records_count,
        )


    ## Prepare a DataFrame before writing to Silver.
    # - Remove Bronze technical columns.
    # - Add Silver technical columns.
    # - Align to the target Silver schema.
    def _prepare_silver_dataframe(
        self,
        df: DataFrame,
        table_name: str,
        silver_run_id: str,
        mode: str,
        category: str,
        allow_new_columns: bool = False,
    ) -> DataFrame:

        df = self._remove_bronze_technical_columns(df)

        df = self._add_silver_technical_columns(
            df=df,
            silver_run_id=silver_run_id,
            mode=mode,
            category=category,
            table_name=table_name
        )

        if allow_new_columns:
            self._add_new_columns_to_target(
                df=df,
                table_name=table_name,
            )

        df = self._align_to_target_schema_strict(
            df=df,
            table_name=table_name,
        )

        return df


    ## Add unexpected source columns to the target Silver table.
    # This method performs additive schema evolution only.
    #
    # Rules:
    # - Existing columns are not changed.
    # - Existing columns are not removed.
    # - New columns are added as nullable Delta columns.
    def _add_new_columns_to_target(
        self,
        df: DataFrame,
        table_name: str,
    )  -> bool:

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
                (
                    f"{field.name} {field.dataType.simpleString()}"
                )
                for field in new_fields
            ]
        )

        self.spark.sql(
            f"""
            ALTER TABLE {table_name}
            ADD COLUMNS ({column_definitions})
            """
        )

        added_columns = [field.name for field in new_fields]

        self._log(
            "warning",
            f"New columns automatically added to Setup Silver table. "
            f"table={table_name}, columns={added_columns}. "
            "The project Silver DDL should be updated accordingly.",
        )

        return True


    ## Remove Bronze-only technical columns.
    def _remove_bronze_technical_columns(
        self,
        df: DataFrame,
    ) -> DataFrame:

        columns_to_drop = [
            col_name
            for col_name in (
                    [
                    self.bronze_run_id_column,
                    self.bronze_ingested_at_column,
                ]
                + self.tran_fields
            )
            if col_name in df.columns
        ]

        if columns_to_drop:
            return df.drop(*columns_to_drop)

        return df


    ## Add Silver technical columns.
    # mode:
    # - insert: used for append/full_replace/new records.
    # - upsert: used for SCD1 merge input.
    def _add_silver_technical_columns(
        self,
        df: DataFrame,
        silver_run_id: str,
        mode: str,
        category: str,
        table_name: str,
    ) -> DataFrame:

        df = df.withColumn(
            self.silver_run_id_column,
            F.lit(silver_run_id),
        )

        if mode == "insert":
            df = df.withColumn(
                self.silver_created_at_column,
                F.current_timestamp(),
            )

        df = df.withColumn(
            self.silver_updated_at_column,
            F.current_timestamp(),
        )

        if category != "full_replace":
            df = df.withColumn(
                self.silver_is_deleted_column,
                F.lit(False),
            )

            df = df.withColumn(
                self.silver_deleted_at_column,
                F.lit(None).cast("timestamp"),
            )

        return df


    ## Align source DataFrame to target table schema.
    # - Existing columns are cast to target type.
    # - Missing target columns are added as NULL and cast to target type.
    # - Unexpected source columns fail the write.
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
                "Target schema is contractual. Please update the Silver table schema explicitly "
                "before processing these fields."
            )

        for field in target_schema.fields:
            field_lower = field.name.lower()

            if field_lower not in source_col_map:
                df = df.withColumn(
                    field.name,
                    F.lit(None).cast(field.dataType),
                )
            else:
                df = df.withColumn(
                    field.name,
                    F.col(source_col_map[field_lower]).cast(field.dataType),
                )

        return df.select(*target_columns)


    # Deduplicate records against the last successful run.
    # This is used to prevent duplicate records from being written to Silver.
    def _deduplicate_against_last_success_run(
        self,
        source_df: DataFrame,
        table_name: str,
        key_columns: List[str],
        max_silver_run_id: Optional[str],
    ) -> DataFrame:

        if not max_silver_run_id:
            return source_df

        target_df = (
            self.spark.table(table_name)
            .filter(F.col("silver_run_id") == F.lit(max_silver_run_id))
            .select(*key_columns)
            .dropDuplicates(key_columns)
        )

        return (
            source_df.alias("s")
            .join(
                target_df.alias("t"),
                on=key_columns,
                how="left_anti"
            )
        )


    ## Convert input data into a Spark DataFrame.
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


    ## Validate that the target Silver table exists.
    def _validate_target_table(
        self,
        table_name: str,
    ) -> None:

        if not self.spark.catalog.tableExists(table_name):
            raise ValueError(f"Target Silver table does not exist: {table_name}")


    ## Check whether a DataFrame is empty.
    def _is_empty_dataframe(
        self,
        df: DataFrame,
    ) -> bool:

        return len(df.take(1)) == 0


    ## Count records safely for logging.
    def _safe_count(
        self,
        df: DataFrame,
    ) -> int:

        return df.count()


    ## Build a standard writer result.
    def _result(
        self,
        status: str,
        table_name: str,
        mode: str,
        silver_run_id: str,
        records_written: int,
    ) -> Dict[str, Any]:

        return {
            "status": status,
            "table_name": table_name,
            "mode": mode,
            "silver_run_id": silver_run_id,
            "records_written": records_written,
        }


    ## Log a message if a logger is configured.
    def _log(
        self,
        level: str,
        message: str,
    ) -> None:

        if self.logger is None:
            return

        log_method = getattr(self.logger, level, None)

        if callable(log_method):
            log_method(message)