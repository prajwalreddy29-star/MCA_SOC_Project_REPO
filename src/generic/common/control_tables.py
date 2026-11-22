from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    BooleanType
)


STATE_SCHEMA = StructType([
    StructField("endpoint_id", StringType(), False),
    StructField("initial_load_completed", BooleanType(), True),
    StructField("last_status", StringType(), True),
    StructField("last_success_bronze_run_id", StringType(), True),
    StructField("last_success_at", TimestampType(), True),
    StructField("last_cursor", StringType(), True),
    StructField("last_from_datetime", TimestampType(), True),
    StructField("last_to_datetime", TimestampType(), True),
    StructField("last_records_fetched", IntegerType(), True),
    StructField("last_records_written", IntegerType(), True),
    StructField("last_error_message", StringType(), True)
])


## Manage ingestion control tables.
# - Read endpoint configuration.
# - Insert and update ingestion run logs.
# - Insert and update state tables.
# - Manage silver processing queue.
# - ....
class ControlTableHelper:

    def __init__(
        self,
        spark: SparkSession,
        config_table: str,
        run_log_table: str,
        silver_config_table: str,
        silver_processing_queue_table: str,
        state_table: Optional[str] = None,
        restriction_processing_queue_table: Optional[str] = None,
        restriction_log_table: Optional[str] = None,

    ) -> None:
        self.spark = spark
        self.config_table = config_table
        self.run_log_table = run_log_table
        self.silver_config_table = silver_config_table
        self.silver_processing_queue_table = silver_processing_queue_table
        self.state_table = state_table
        self.restriction_processing_queue_table = restriction_processing_queue_table
        self.restriction_log_table = restriction_log_table

    ## SETUP - Return active setup endpoint configurations.
    # - Optional country code filter.
    def get_active_setup_endpoints(
        self,
        countries: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        
        df = (
            self.spark.table(self.config_table)
            .filter(F.col("is_active") == F.lit(True))
            .filter(F.col("controller_type") == F.lit("setup"))
        )

        if countries:
            df = df.filter(F.col("country_code").isin(countries))

        return [row.asDict(recursive=True) for row in df.collect()]


    ## CDP - Return active initial (cdp) endpoint configurations.
    # - Mode : Initial / incremental
    # - Optional country code filter.   
    def get_active_cdp_endpoints(
        self,
        countries: Optional[List[str]] = None,
        ingestion_mode: Optional[str] = None
    ) -> List[Dict[str, Any]]:

        df = (
            self.spark.table(self.config_table)
            .filter(F.col("is_active") == F.lit(True))
            .filter(F.col("controller_type") == F.lit("cdp"))
            .filter(F.col("ingestion_mode") == F.lit(ingestion_mode))
        )

        if countries:
            df = df.filter(F.col("country_code").isin(countries))

        return [row.asDict(recursive=True) for row in df.collect()]



    # INITIAL - Return states for which initial load was never completed.
    def get_active_cdp_initial_endpoints_never_loaded(
        self,
        countries: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        
        state_df = self.spark.table(self.state_table).filter(F.col("initial_load_completed") == False)
        config_df = self.spark.table(self.config_table)
        df = (
            state_df.select("endpoint_id")
            .join(
                config_df,
                on="endpoint_id"
            )
            .filter(F.col("is_active") == F.lit(True))
            .filter(F.col("controller_type") == F.lit("cdp"))
            .filter(F.col("ingestion_mode") == F.lit("initial"))
        )

        if countries:
            df = df.filter(F.col("country_code").isin(countries))

        return [row.asDict(recursive=True) for row in df.collect()]
    
    
    # CDP - Get states information for a specific endpoint 
    def get_endpoint_state(
    self,
    endpoint_id: str,
    ) -> Dict[str, Any]:

        df = (
            self.spark.table(self.state_table)
            .filter(F.col("endpoint_id") == F.lit(endpoint_id))
            .limit(1)
        )

        rows = df.collect()

        if not rows:
            raise ValueError(f"No ingestion state found for endpoint_id={endpoint_id}")

        return rows[0].asDict(recursive=True)


    # Update endpoint state table for a given endpoint_id
    def update_endpoints_state(
        self,
        endpoint_id: str,
        initial_load_completed: Optional[bool] = None,
        last_status: Optional[str] = None,
        last_success_bronze_run_id: Optional[str] = None,
        last_success_at: Optional[str] = None,
        last_cursor: Optional[str] = None,
        last_from_datetime: Optional[str] = None,
        last_to_datetime: Optional[str] = None,
        last_records_fetched: Optional[int] = None,
        last_records_written: Optional[int] = None,
        last_error_message: Optional[str] = None,
    ) -> None:
        updates = {}
        if initial_load_completed is not None:
            updates["initial_load_completed"] = initial_load_completed
        if last_status is not None:
            updates["last_status"] = last_status
        if last_success_bronze_run_id is not None:
            updates["last_success_bronze_run_id"] = last_success_bronze_run_id
        if last_success_at is not None:
            updates["last_success_at"] = last_success_at
        if last_cursor is not None:
            updates["last_cursor"] = last_cursor
        if last_from_datetime is not None:
            updates["last_from_datetime"] = last_from_datetime
        if last_to_datetime is not None:
            updates["last_to_datetime"] = last_to_datetime
        if last_records_fetched is not None:
            updates["last_records_fetched"] = last_records_fetched
        if last_records_written is not None:
            updates["last_records_written"] = last_records_written
        if last_error_message is not None:
            updates["last_error_message"] = last_error_message

        if updates:
            set_expr = ", ".join([f"{k} = {repr(v)}" for k, v in updates.items()])
            query = f"""
                UPDATE {self.state_table}
                SET {', '.join([f"{k} = :{k}" for k in updates.keys()])}
                WHERE endpoint_id = :endpoint_id
            """
            params = updates.copy()
            params["endpoint_id"] = endpoint_id
            self.spark.sql(query, params)


    # Normalize state row to expected schema
    def normalize_state_row(
        self,
        row: Dict[str, Any]
    ) -> Dict[str, Any]:

        return {
            "endpoint_id": row.get("endpoint_id"),
            "initial_load_completed": row.get("initial_load_completed"),
            "last_status": row.get("last_status"),
            "last_success_bronze_run_id": row.get("last_success_bronze_run_id"),
            "last_success_at": row.get("last_success_at"),
            "last_cursor": row.get("last_cursor"),
            "last_from_datetime": row.get("last_from_datetime"),
            "last_to_datetime": row.get("last_to_datetime"),
            "last_records_fetched": row.get("last_records_fetched"),
            "last_records_written": row.get("last_records_written"),
            "last_error_message": row.get("last_error_message")
        }
   
    
    # update state rows in one Delta write.
    # This method should be called by the orchestrator, not by each worker
    # SUCCESS rows update watermarks/cursors.
    # FAILED rows update status/error only.
    # Previous successful state is preserved on failure.
    def update_run_states(
        self,
        rows: List[Dict[str, Any]],
    ) -> None:

        if not rows:
            return

        normalized_rows = [self.normalize_state_row(row) for row in rows]
        updates_df = self.spark.createDataFrame(normalized_rows,schema=STATE_SCHEMA)
        target = DeltaTable.forName(self.spark, self.state_table)
        (
            target.alias("t")
            .merge(
                updates_df.alias("s"),
                "t.endpoint_id = s.endpoint_id"
            )
            .whenMatchedUpdate(set={
                # Always update operational status
                "last_status": "s.last_status",
                "last_records_fetched": "s.last_records_fetched",
                "last_records_written": "s.last_records_written",
                "last_error_message": "s.last_error_message",

                # Update these fields only on SUCCESS
                "initial_load_completed": """
                    CASE WHEN s.initial_load_completed IS NOT NULL
                    THEN s.initial_load_completed
                    ELSE t.initial_load_completed
                    END
                """,
                "last_success_bronze_run_id": """
                    CASE WHEN s.last_status = 'SUCCESS'
                    THEN s.last_success_bronze_run_id
                    ELSE t.last_success_bronze_run_id
                    END
                """,
                "last_success_at": """
                    CASE WHEN s.last_status = 'SUCCESS'
                    THEN s.last_success_at
                    ELSE t.last_success_at
                    END
                """,
                "last_cursor": """
                    CASE WHEN s.last_status = 'SUCCESS'
                    THEN s.last_cursor
                    ELSE t.last_cursor
                    END
                """,
                "last_from_datetime": """
                    CASE WHEN s.last_status = 'SUCCESS'
                    THEN s.last_from_datetime
                    ELSE t.last_from_datetime
                    END
                """,
                "last_to_datetime": """
                    CASE WHEN s.last_status = 'SUCCESS'
                    THEN s.last_to_datetime
                    ELSE t.last_to_datetime
                    END
                """,
            })
            .execute()
        )

    
    # Initialize incremental state rows after successful initial loads.
    # One input row is expected per entity_name / country_code.
    def init_incremental_states_batch(
        self,
        rows: List[Dict[str, Any]],
    ) -> None:

        if not rows:
            return

        input_schema = StructType([
            StructField("entity_name", StringType(), False),
            StructField("country_code", StringType(), True),
            StructField("last_to_datetime", TimestampType(), True),
        ])

        input_df = self.spark.createDataFrame(rows, schema=input_schema)

        config_df = (
            self.spark.table(self.config_table)
            .filter(F.col("is_active") == F.lit(True))
            .filter(F.col("controller_type") == F.lit("cdp"))
            .filter(F.col("ingestion_mode") == F.lit("incremental"))
            .select(
                "endpoint_id",
                "entity_name",
                "country_code",
            )
        )

        updates_df = (
            config_df.alias("cfg")
            .join(
                input_df.alias("i"),
                (F.col("cfg.entity_name") == F.col("i.entity_name")) &
                (F.col("cfg.country_code") == F.col("i.country_code")),
                "inner",
            )
            .select(
                F.col("cfg.endpoint_id").alias("endpoint_id"),
                F.col("i.last_to_datetime").alias("last_to_datetime"),
            )
            .dropDuplicates(["endpoint_id"])
        )

        # If empty df
        if updates_df.take(1) == []:
            return

        target = DeltaTable.forName(self.spark, self.state_table)

        (
            target.alias("t")
            .merge(
                updates_df.alias("s"),
                "t.endpoint_id = s.endpoint_id"
            )
            .whenMatchedUpdate(set={
                "last_to_datetime": "s.last_to_datetime"
            })
            .execute()
        )



    #Append run log rows in one Delta write.
    #This method should be called by the orchestrator, not by each worker
    def append_run_logs(
        self,
        rows: List[Dict[str, Any]],
    ) -> None:

        if not rows:
            return
        
        target_schema = self.spark.table(self.run_log_table).schema
        
        # Keep only fields defined in the target log table.
        # Missing fields are automatically filled with None.

        normalized_rows = [
        {
            field.name: row.get(field.name)
            for field in target_schema.fields
        }
        for row in rows
        ]

        df = self.spark.createDataFrame(
            normalized_rows,
            schema=target_schema,
        )
        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(self.run_log_table)
        )
    

    # -------------------------------------------------------------------------
    # Silver configuration
    # -------------------------------------------------------------------------

    # Insert Silver queue items for a successfully completed Bronze endpoint.
    # One queue item is created for each active Silver task linked to the
    # given Bronze endpoint. Performed in a batch mode to avoid parallel merge error       
    def enqueue_silver_tasks_batch(
        self,
        bronze_success_items: List[Dict[str, Any]]
    ) -> None:
        
        if not bronze_success_items:
            return

        input_schema = StructType([
            StructField("bronze_run_id", StringType(), False),
            StructField("endpoint_id", StringType(), False),
        ])

        input_df = self.spark.createDataFrame(
            bronze_success_items,
            schema=input_schema,
        ).dropDuplicates(["bronze_run_id", "endpoint_id"])

        cfg_df = (
            self.spark.table(self.silver_config_table)
            .filter(F.col("is_active") == F.lit(True))
            .select(
                "silver_task_id",
                "source_endpoint_id",
            )
            .dropDuplicates(["silver_task_id", "source_endpoint_id"])
        )

        queue_df = (
            input_df.alias("i")
            .join(
                cfg_df.alias("cfg"),
                F.col("i.endpoint_id") == F.col("cfg.source_endpoint_id"),
                "inner",
            )
            .withColumn(
                "queue_id",
                F.concat_ws(
                    "_",
                    F.col("cfg.silver_task_id"),
                    F.col("i.bronze_run_id"),
                )
            )
            .withColumn("status", F.lit("PENDING"))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("started_at", F.lit(None).cast("timestamp"))
            .withColumn("updated_at", F.current_timestamp())
            .withColumn("last_silver_run_id", F.lit(None).cast("string"))
            .withColumn("last_error_message", F.lit(None).cast("string"))
            .select(
                "queue_id",
                F.col("i.bronze_run_id").alias("bronze_run_id"),
                F.col("cfg.silver_task_id").alias("silver_task_id"),
                "status",
                "created_at",
                "started_at",
                "updated_at",
                "last_silver_run_id",
                "last_error_message",
            )
            .dropDuplicates(["queue_id"])
        )

        # If empty df
        if queue_df.take(1) == []:
            return

        target = DeltaTable.forName(
            self.spark,
            self.silver_processing_queue_table,
        )

        (
            target.alias("t")
            .merge(
                queue_df.alias("s"),
                "t.queue_id = s.queue_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )


    # Return active Silver processing tasks from silver_processing_config.
    # Optional filter on silver_task_ids .
    def get_active_silver_tasks(
        self,
        silver_task_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:

        df = (
            self.spark.table(self.silver_config_table)
            .filter(F.col("is_active") == F.lit(True))
        )

        if silver_task_ids:
            df = df.filter(F.col("silver_task_id").isin(silver_task_ids))

        return [row.asDict(recursive=True) for row in df.collect()]


    # Retrieve pending Silver queue items with their Silver configuration,
    # mark selected items as PROCESSING, and return them as work items.
    # if ("setup"):
    #   - keep only the latest PENDING queue item per silver_task_id.
    #   - this is useful for setup full-load tasks where older pending runs can be skipped
    # Returns : Work items enriched with Silver processing configuration.
    def get_pending_silver_work_items(
        self,
        silver_run_id: str,
        ingestion_mode: str,  # Logical load group to process ("full", "initial", "incremental")
    ) -> List[Dict[str, Any]]:
        
        queue_df = (
            self.spark.table(self.silver_processing_queue_table)
            .filter(F.col("status") == F.lit("PENDING"))
            .alias("q")
        )

        silver_config_df = (
            self.spark.table(self.silver_config_table)
            .filter(F.col("is_active") == F.lit(True))                
            .alias("slv_cfg")
        )
                    
        endpoint_config_df = (
            self.spark.table(self.config_table)
            .filter(F.col("is_active") == F.lit(True))
            .filter(F.col("ingestion_mode") == F.lit(ingestion_mode))
            .alias("endp_cfg")
        )

        pending_df = (
            queue_df
            .join(
                silver_config_df,
                F.col("q.silver_task_id") == F.col("slv_cfg.silver_task_id"),
                "inner",
            )
            .join(
                endpoint_config_df,
                F.col("slv_cfg.source_endpoint_id") == F.col("endp_cfg.endpoint_id"),
                "inner",
            )
            .select(
                F.col("q.queue_id"),
                F.col("q.bronze_run_id"),
                F.col("q.silver_task_id"),
                F.col("q.status"),
                F.col("q.created_at"),
                F.col("q.started_at"),
                F.col("q.updated_at"),
                F.col("q.last_silver_run_id"),
                F.col("q.last_error_message"),

                F.col("slv_cfg.source_endpoint_id"),
                F.col("slv_cfg.country_code"),
                F.col("slv_cfg.source_bronze_table"),
                F.col("slv_cfg.target_silver_table"),
                F.col("slv_cfg.processing_pattern"),
                F.col("slv_cfg.processing_order"),
                F.col("slv_cfg.primary_key_columns"),
                F.col("slv_cfg.dedup_order_columns"),

                F.col("endp_cfg.ingestion_mode"),
                F.col("endp_cfg.entity_name"),
            )
        )

        # For setup full-load tasks, keep only the latest pending item per silver_task_id.
        # Older pending items can be marked as SKIPPED.
        if ingestion_mode == "full":
            w = Window.partitionBy("silver_task_id").orderBy(F.col("created_at").desc())

            ranked_df = (
                pending_df
                .withColumn("_rn", F.row_number().over(w))
            )

            skipped_df = ranked_df.filter(F.col("_rn") > 1)
            pending_df = ranked_df.filter(F.col("_rn") == 1).drop("_rn")

            skipped_queue_ids = [
                row["queue_id"]
                for row in skipped_df.select("queue_id").collect()
            ]

            if skipped_queue_ids:
                self.update_silver_queue_status(
                    queue_ids=skipped_queue_ids,
                    status="SKIPPED",
                    silver_run_id=silver_run_id,
                    error_message="Skipped because a newer pending full-load exists for the same silver_task_id.",
                )

        pending_df = pending_df.orderBy(
            F.col("entity_name"),
            F.col("processing_order"),
            F.col("created_at"),
        )

        pending_rows = [
            row.asDict(recursive=True)
            for row in pending_df.collect()
        ]

        if not pending_rows:
            return []

        queue_ids = [row["queue_id"] for row in pending_rows]

        # Mark selected tasks as PROCESSING.
        # This acts as a logical lock for the current Silver run.
        self.update_silver_queue_status(
            queue_ids=queue_ids,
            status="PROCESSING",
            silver_run_id=silver_run_id,
            error_message=None
        )

        return pending_rows
    
    # Return the last successful Silver run id for a given entity.
    # This is useful for incremental Silver runs like event_history. This value will be used then to filter on only the last successful run for the entity in deduplicate logic.
    def get_last_success_silver_run_id_for_entity(
        self,
        entity_name: str,
    ) -> Optional[str]:

        queue_df = self.spark.table(self.silver_processing_queue_table)
        config_df = self.spark.table(self.silver_config_table)

        df = (
            queue_df.alias("q")
            .join(
                config_df.alias("c"),
                F.col("q.silver_task_id") == F.col("c.silver_task_id"),
                "inner"
            )
            .filter(F.col("q.status") == F.lit("SUCCESS"))
            .filter(F.col("c.entity_name") == F.lit(entity_name))
            .select(F.col("q.last_silver_run_id"))
        )

        row = df.agg(F.max("last_silver_run_id").alias("max_silver_run_id")).collect()[0]

        return row["max_silver_run_id"]



    # Update Silver queue status for a list of queue_ids.
    # This method assumes queue rows already exist.
    # It should be used for state transitions and timestamps :
    #    - PROCESSING, SUCCESS, PENDING, SKIPPED
    def update_silver_queue_status(
        self,
        queue_ids: List[str],
        status: str,
        silver_run_id: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:

        if not queue_ids:
            return

        target = DeltaTable.forName(
            self.spark,
            self.silver_processing_queue_table,
        )

        update_set = {
            "status": F.lit(status),
            "updated_at": F.current_timestamp(),
        }

        if silver_run_id is not None:
            update_set["last_silver_run_id"] = F.lit(silver_run_id)

        if error_message is not None:
            update_set["last_error_message"] = F.lit(error_message)
        elif status in ("SUCCESS", "PROCESSING"):
            update_set["last_error_message"] = F.lit(None).cast("string")

        if status == "PROCESSING":
            update_set["started_at"] = F.current_timestamp()

        (
            target.update(
                condition=F.col("queue_id").isin(queue_ids),
                set=update_set,
            )
        )


    # -------------------------------------------------------------------------
    # Restriction Actions configuration
    # -------------------------------------------------------------------------

    # Insert restriction processing actions into restriction_processing_queue.
    # rows contains restrictions changes to process ("INCLUDE" or "EXCLUDE")
    # If already exists, don't create it again.
    def enqueue_restriction_tasks(
        self,
        rows: List[Dict[str, Any]],
    ) -> None:

        if not rows:
            return

        schema = StructType([
            StructField("pipeline_run_id", StringType(), True),
            StructField("bronze_run_id", StringType(), False),
            StructField("site_no", StringType(), False),
            StructField("restriction_action", StringType(), False),
            StructField("source_endpoint_id", StringType(), False),
        ])

        input_df = (
            self.spark.createDataFrame(rows, schema=schema)
            .withColumn(
                "restriction_queue_id",
                F.concat_ws(
                    "_",
                    F.col("bronze_run_id"),
                    F.col("site_no"),
                    F.col("restriction_action"),
                )
            )
            .withColumn("status", F.lit("PENDING"))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("started_at", F.lit(None).cast("timestamp"))
            .withColumn("updated_at", F.current_timestamp())
            .withColumn("last_error_message", F.lit(None).cast("string"))
            .select(
                "restriction_queue_id",
                "pipeline_run_id",
                "bronze_run_id",
                "site_no",
                "restriction_action",
                "status",
                "source_endpoint_id",
                "created_at",
                "started_at",
                "updated_at",
                "last_error_message",
            )
            .dropDuplicates(["restriction_queue_id"])
        )

        # If empty df
        if input_df.take(1) == []:
            return

        target = DeltaTable.forName(
            self.spark,
            self.restriction_processing_queue_table,
        )

        (
            target.alias("t")
            .merge(
                input_df.alias("s"),
                "t.restriction_queue_id = s.restriction_queue_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    

    # Return pending restriction processing queue items.
    # restriction_action:
    #  - None: return all pending actions
    #  - "INCLUDE": return only include actions
    #  - "EXCLUDE": return only exclude actions
    def get_pending_restriction_processing_items(
        self,
        restriction_action: str = None,
    ) -> List[Dict[str, Any]]:
    
        df = (
            self.spark.table(self.restriction_processing_queue_table)
            .filter(F.col("status") == F.lit("PENDING"))
        )

        if restriction_action:
            df = df.filter(F.col("restriction_action") == F.lit(restriction_action))

        df = df.orderBy("created_at")

        return [row.asDict(recursive=True) for row in df.collect()]

    
    # Update restriction_processing_queue status for a list of queue ids.
    #  Used statuses:
    #    - PROCESSING
    #    - SUCCESS
    #    - FAILED
    #    - SKIPPED
    def update_restriction_queue_status(
        self,
        restriction_queue_ids: List[str],
        status: str,
        error_message: str = None,
    ) -> None:
        
        if not restriction_queue_ids:
            return

        ids_sql = ",".join([f"'{x}'" for x in restriction_queue_ids])

        if status == "PROCESSING":
            query = f"""
                UPDATE {self.restriction_processing_queue_table}
                SET
                    status = :status,
                    started_at = current_timestamp(),
                    updated_at = current_timestamp(),
                    last_error_message = NULL
                WHERE restriction_queue_id IN ({ids_sql})
            """
            params = {"status": status}

        else:
            query = f"""
                UPDATE {self.restriction_processing_queue_table}
                SET
                    status = :status,
                    updated_at = current_timestamp(),
                    last_error_message = :error_message
                WHERE restriction_queue_id IN ({ids_sql})
            """
            params = {
                "status": status,
                "error_message": error_message,
            }

        self.spark.sql(query, params)

    # Append restriction processing log rows.    
    #This method should be called by the orchestrator, not by each worker
    def append_restriction_logs(
        self,
        rows: List[Dict[str, Any]],
    ) -> None:    

        if not rows:
            return

        target_schema = self.spark.table(self.restriction_log_table).schema

        df = self.spark.createDataFrame(
            rows,
            schema=target_schema,
        )
        
        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(self.restriction_log_table)
        )