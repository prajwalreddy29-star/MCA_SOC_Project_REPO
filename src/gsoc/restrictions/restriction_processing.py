from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from generic.common.run_context import RunContext
from generic.common.control_tables import ControlTableHelper
from gsoc.bronze.bronze_writer import BronzeWriter
from generic.ssae_api.ssae_client import SsaeApiClient

## Process restriction actions detected from SSAE restriction changes.
# Responsibilities:
# - Read PENDING actions from restriction_processing_queue.
# - Process INCLUDE and EXCLUDE actions directly.
# - INCLUDE:
#     - Retrieve the current Site and Systems from SSAE.
#     - Write the source responses to Bronze.
#     - Immediately upsert the new Bronze rows into Silver.
#     - Do not enqueue standard Silver processing tasks.
# - EXCLUDE:
#     - Build the complete Site/System/Incident scope before modifying data.
#     - Apply soft or hard deletes directly in Silver.
#     - Process child entities before parent entities.
# - Write one restriction processing log row per target entity.
# - Update the restriction queue status.
#
# Important:
# - Existing Bronze data is not deleted during EXCLUDE processing.
# - Historical transactional data is not reloaded during INCLUDE processing.
# - INCLUDE directly reactivates/upserts Site and System in Silver.
# - Operations across several Delta tables are not globally transactional.
# - Every operation must therefore be idempotent and safe to retry.

class RestrictionProcessingService:

    ## Initialize the restriction processing service.
    #
    # Configuration example for country-specific schemas:
    # {
    #     "GSOC": {
    #         "api_country_code": "GC_EU",
    #         "site_bronze_table": "cdp_data_gsoc_dev.bronze_gsoc_operational.site",
    #         "system_bronze_table": "cdp_data_gsoc_dev.bronze_gsoc_operational.system",
    #         "site_silver_table": "cdp_data_gsoc_dev.silver_gsoc_operational.site",
    #           ...
    #     }
    # }
    #    
    def __init__(
        self,
        spark: SparkSession,
        client: SsaeApiClient,
        bronze_writer: BronzeWriter,
        control_helper: ControlTableHelper,
        restriction_config: Dict[str, Dict[str, Any]],
        delete_mode: str = "SOFT",        
        logger: Optional[Any] = None,
    ) -> None:
        self.spark = spark
        self.client = client
        self.bronze_writer = bronze_writer
        self.control_helper = control_helper
        self.restriction_config = restriction_config
        self.delete_mode = delete_mode.upper()
        self.logger = logger

        if self.delete_mode not in ("SOFT", "HARD"):
            raise ValueError("delete_mode must be either 'SOFT' or 'HARD'")

    ## Process pending restriction actions.
    # restriction_action can be None, INCLUDE or EXCLUDE.
    # Actions are processed sequentially because restrictions are rare and
    # several actions can modify the same Silver tables.
    def run(
        self,
        context: RunContext,
        restriction_action: Optional[str] = None,
    ) -> Dict[str, Any]:
        if restriction_action:
            restriction_action = restriction_action.upper()
            if restriction_action not in ("INCLUDE", "EXCLUDE"):
                raise ValueError(
                    "restriction_action must be INCLUDE, EXCLUDE or None"
                )

        pending_items = (
            self.control_helper.get_pending_restriction_processing_items(
                restriction_action=restriction_action
            )
        )

        if not pending_items:
            return {
                "pipeline_run_id": context.pipeline_run_id,
                "restriction_run_id": context.run_id,
                "delete_mode": self.delete_mode,
                "total_actions": 0,
                "success_count": 0,
                "failure_count": 0,
                "message": "No pending restriction actions found.",
            }

        results: List[Dict[str, Any]] = []
        pending_logs: List[Dict[str, Any]] = []

        self.control_helper.update_restriction_queue_status(
                restriction_queue_ids=[item["restriction_queue_id"] for item in pending_items],
                status="PROCESSING",
                error_message=None,
        )

        for item in pending_items:

            queue_id = item["restriction_queue_id"]
            result = self._process_item(item=item, context=context)
            results.append(result)
            pending_logs.extend(result.get("logs", []))

            if result["status"] == "SUCCESS":
                self.control_helper.update_restriction_queue_status(
                    restriction_queue_ids=[queue_id],
                    status="SUCCESS",
                    error_message=None,
                )
            else:
                self.control_helper.update_restriction_queue_status(
                    restriction_queue_ids=[queue_id],
                    status="PENDING",
                    error_message=result["error_message"],
                )

        if pending_logs:
            self.control_helper.append_restriction_logs(pending_logs)

        success_count = sum(
            1 for result in results if result["status"] == "SUCCESS"
        )

        return {
            "pipeline_run_id": context.pipeline_run_id,
            "restriction_run_id": context.run_id,
            "delete_mode": self.delete_mode,
            "total_actions": len(results),
            "success_count": success_count,
            "failure_count": len(results) - success_count,
            "failures": [
                {
                    "restriction_queue_id": result["restriction_queue_id"],
                    "site_no": result["site_no"],
                    "restriction_action": result["restriction_action"],
                    "error_message": result["error_message"],
                }
                for result in results
                if result["status"] == "FAILED"
            ],
        }

    ## Process one restriction item.
    def _process_item(
        self,
        item: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:
        action = str(item["restriction_action"]).upper()
        logs: List[Dict[str, Any]] = []

        try:            
            config = self.restriction_config

            if action == "INCLUDE":
                self._process_include(
                    item=item,
                    context=context,
                    config=config,
                    logs=logs,
                )
            elif action == "EXCLUDE":
                self._process_exclude(
                    item=item,
                    context=context,
                    config=config,
                    logs=logs,
                )
            else:
                raise ValueError(f"Unsupported restriction_action={action}")

            return {
                "restriction_queue_id": item["restriction_queue_id"],
                "site_no": item["site_no"],
                "restriction_action": action,
                "status": "SUCCESS",
                "error_message": "",
                "logs": logs,
            }

        except Exception as exc:
            return {
                "restriction_queue_id": item["restriction_queue_id"],
                "site_no": item.get("site_no"),
                "restriction_action": action,
                "status": "FAILED",
                "error_message": str(exc),
                "logs": logs,
            }

    # -------------------------------------------------------------------------
    # INCLUDE processing
    # -------------------------------------------------------------------------

    ## Re-ingest Site and Systems, then update Silver directly.
    # No standard Silver queue item is created for this restriction action.
    def _process_include(
        self,
        item: Dict[str, Any],
        context: RunContext,
        config: Dict[str, Any],
        logs: List[Dict[str, Any]],
    ) -> None:
        site_no = item["site_no"]

        bronze_context = RunContext(
            layer="bronze",
            load_group="restriction",
            load_phase="include",
            pipeline_run_id=context.pipeline_run_id,
        )

        site_log = self._include_site(
            item=item,
            restriction_context=context,
            bronze_context=bronze_context,
            site_no=site_no,
            config=config,
        )
        logs.append(site_log)

        if site_log["status"] == "FAILED":
            raise RuntimeError(
                f"INCLUDE site failed for site_no={site_no}: "
                f"{site_log['error_message']}"
            )

        systems_log = self._include_systems(
            item=item,
            restriction_context=context,
            bronze_context=bronze_context,
            site_no=site_no,
            config=config,
        )
        logs.append(systems_log)

        if systems_log["status"] == "FAILED":
            raise RuntimeError(
                f"INCLUDE systems failed for site_no={site_no}: "
                f"{systems_log['error_message']}"
            )

    ## Retrieve one Site, write it to Bronze and directly upsert it in Silver.
    def _include_site(
        self,
        item: Dict[str, Any],
        restriction_context: RunContext,
        bronze_context: RunContext,
        site_no: int,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        started_at = datetime.now(timezone.utc)
        records_affected = 0

        try:
            
            site = self.client.get_uniquesite(
                country=config["api_country_code"],
                site_no=site_no,
            )

            if site is None:
                raise ValueError(
                    f"Site not found in SSAE for site_no={site_no}"
                )
        
            debug = self.bronze_writer.write(
                data=[site],
                table_name=config["site_bronze_table"],
                bronze_run_id=bronze_context.run_id,
                mode="append",
            )


            records_affected = self._upsert_bronze_to_silver(
                bronze_rows=[site],
                target_table=config["site_silver_table"],
                key_column="site_no",
                restriction_run_id=restriction_context.run_id,
            )

            ended_at = datetime.now(timezone.utc)
            return self._build_log(
                item=item,
                context=restriction_context,
                action="INCLUDE",
                target_entity="site",
                started_at=started_at,
                ended_at=ended_at,
                status="SUCCESS",
                records_affected=records_affected,
                error_message="",
            )

        except Exception as exc:
            ended_at = datetime.now(timezone.utc)
            return self._build_log(
                item=item,
                context=restriction_context,
                action="INCLUDE",
                target_entity="site",
                started_at=started_at,
                ended_at=ended_at,
                status="FAILED",
                records_affected=records_affected,
                error_message=str(exc),
            )


    ## Retrieve Systems, write them to Bronze and directly upsert them in Silver.
    def _include_systems(
        self,
        item: Dict[str, Any],
        restriction_context: RunContext,
        bronze_context: RunContext,
        site_no: int,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        started_at = datetime.now(timezone.utc)
        records_affected = 0

        try:
            systems = list(
                self.client.get_uniquesite_systems(
                    country=config["api_country_code"],
                    site_no=site_no,
                ) or []
            )
            if systems == []:
                raise ValueError(
                    f"Systems not found in SSAE for site_no={site_no}"
                )

            else:
                self.bronze_writer.write(
                    data=systems,
                    table_name=config["system_bronze_table"],
                    bronze_run_id=bronze_context.run_id,
                    mode="append",
                )

                records_affected = self._upsert_bronze_to_silver(
                    bronze_rows = systems,
                    target_table=config["system_silver_table"],
                    key_column="system_no",
                    restriction_run_id=restriction_context.run_id,                    
                )

            ended_at = datetime.now(timezone.utc)
            return self._build_log(
                item=item,
                context=restriction_context,
                action="INCLUDE",
                target_entity="system",
                started_at=started_at,
                ended_at=ended_at,
                status="SUCCESS",
                records_affected=records_affected,
                error_message="",
            )

        except Exception as exc:
            ended_at = datetime.now(timezone.utc)
            return self._build_log(
                item=item,
                context=restriction_context,
                action="INCLUDE",
                target_entity="system",
                started_at=started_at,
                ended_at=ended_at,
                status="FAILED",
                records_affected=records_affected,
                error_message=str(exc),
            )           


    ## Directly upsert Bronze rows into one Silver table.
    # Existing rows are fully updated and reactivated.
    # New rows are inserted as active Silver records.
    # No Silver queue item is generated.
    def _upsert_bronze_to_silver(
        self,
        bronze_rows: Sequence[Dict[str, Any]],
        target_table: str,
        key_column: str,
        restriction_run_id: str
    ) -> int:
        if not key_column:
            raise ValueError(
                f"Missing key columns for target_table={target_table}"
            )

        if not bronze_rows:
            return 0

        source_df = self.spark.createDataFrame(bronze_rows)

        records_affected = len(bronze_rows)
        
        timestamp = datetime.now(timezone.utc)

        source_df = source_df.withColumns({
            "silver_run_id": F.lit(restriction_run_id),
            "silver_created_at": F.lit(timestamp),
            "silver_updated_at": F.lit(timestamp),
            "silver_is_deleted": F.lit(False),
            "silver_deleted_at": F.lit(None).cast("timestamp"),
        })

        target = DeltaTable.forName(self.spark, target_table)
        
        merge_condition = f"t.`{key_column}` = s.`{key_column}`"

        columns = source_df.columns  # Cache once to avoid repeated Analyze RPCs

        matched_updates = {
            column: f"s.`{column}`"
            for column in columns
            if column != key_column
            and column != "silver_created_at"
        }
        
        insert_values = {
            column: f"s.`{column}`"
            for column in columns
        }

        (
            target.alias("t")
            .merge(source_df.alias("s"), merge_condition)
            .whenMatchedUpdate(set=matched_updates)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )

        return records_affected


    # -------------------------------------------------------------------------
    # EXCLUDE processing
    # -------------------------------------------------------------------------

    ## Apply an exclusion directly to all related Silver entities.
    # Child-to-parent order:
    # 1. AlarmIncidentCall
    # 2. AlarmIncident
    # 3. EventHistory
    # 4. System
    # 5. Site
    def _process_exclude(
        self,
        item: Dict[str, Any],
        context: RunContext,
        config: Dict[str, Any],
        logs: List[Dict[str, Any]],
    ) -> None:
        site_no = item["site_no"]
        restriction_timestamp = datetime.now(timezone.utc)
        exclude_keys = self._get_exclusion_keys(
            site_no=site_no,
            config=config,
        )
        
        operations = [
            (
                "alarm_incident_call",
                config["alarm_incident_call_silver_table"],
                exclude_keys["alarm_incident_keys"],
                "alarminc_no",
            ),
            (
                "alarm_incident",
                config["alarm_incident_silver_table"],
                exclude_keys["alarm_incident_keys"],
                "alarminc_no",
            ),
            (
                "event_history",
                config["event_history_silver_table"],
                exclude_keys["system_keys"],
                "system_no",
            ),
            (
                "system",
                config["system_silver_table"],
                exclude_keys["system_keys"],
                "system_no",
            ),
            (
                "site",
                config["site_silver_table"],
                exclude_keys["site_keys"],
                "site_no",
            ),
        ]

        has_failure = False

        for entity, table, keys, key_column in operations:
            try:
                log_entry = self._exclude_entity(
                    item=item,
                    context=context,
                    target_entity=entity,
                    target_table=table,
                    keys=keys,
                    key_column=key_column,
                    restriction_timestamp=restriction_timestamp,
                )
                logs.append(log_entry)

                if log_entry["status"] == "FAILED":
                    has_failure = True
            
            except Exception as exc:
                has_failure = True
                now = datetime.now(timezone.utc)
                logs.append(
                    self._build_log(
                        item=item,
                        context=context,
                        action="EXCLUDE",
                        target_entity=entity,
                        started_at=now,
                        ended_at=now,
                        status="FAILED",
                        records_affected=0,
                        error_message=str(exc),
                    )
                )

        if has_failure:
            failed_entities = [
                log["target_entity"]
                for log in logs
                if log["status"] == "FAILED"
            ]
            raise RuntimeError(
                f"EXCLUDE failed for site_no={site_no}, "
                f"entities={failed_entities}"
            )
        

    ## Build Site, System and AlarmIncident key scopes before deletion.
    def _get_exclusion_keys(
        self,
        site_no: int,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        
        site_keys = [site_no]
        
        system_df = (
            self.spark.table(config["system_silver_table"])
            .filter(F.col("site_no") == site_no)
            .select("system_no")
            .distinct()
        )
        system_keys = [row.system_no for row in system_df.collect()]

        system_no_list = [row.system_no for row in system_df.collect()]
        if not system_no_list:
            alarm_incident_keys = []
        else:
            alarm_incident_df = (
                self.spark.table(config["alarm_incident_silver_table"])
                .filter(F.col("system_no").isin(system_no_list))
                .select("alarminc_no")
                .distinct()
            )
            alarm_incident_keys = [row.alarminc_no for row in alarm_incident_df.collect()]
        
        return {
            "site_keys": site_keys,
            "system_keys": system_keys,
            "alarm_incident_keys": alarm_incident_keys,
        }

    
    ## Apply SOFT or HARD deletion to one Silver entity.
    def _exclude_entity(
        self,
        item: Dict[str, Any],
        context: RunContext,
        target_entity: str,
        target_table: str,
        keys: Sequence[Any],  # keys is a list of key value lists
        key_column: Sequence[str],
        restriction_timestamp: datetime,
    ) -> Dict[str, Any]:
        started_at = datetime.now(timezone.utc)
        records_affected = 0

        try:
            
            if self.delete_mode == "SOFT":
                records_affected = self._soft_delete_by_keys(
                    target_table=target_table,
                    keys=keys,
                    key_column=key_column,
                    restriction_run_id=context.run_id,
                    restriction_timestamp=restriction_timestamp,
                )
            else:
                records_affected = self._hard_delete_by_keys(
                    target_table=target_table,
                    keys=keys,
                    key_column=key_column,
                )

            ended_at = datetime.now(timezone.utc)
            return self._build_log(
                item=item,
                context=context,
                action="EXCLUDE",
                target_entity=target_entity,
                started_at=started_at,
                ended_at=ended_at,
                status="SUCCESS",
                records_affected=records_affected,
                error_message="",
            )

        except Exception as exc:
            ended_at = datetime.now(timezone.utc)
            return self._build_log(
                item=item,
                context=context,
                action="EXCLUDE",
                target_entity=target_entity,
                started_at=started_at,
                ended_at=ended_at,
                status="FAILED",
                records_affected=records_affected,
                error_message=str(exc),
            )

    ## Soft-delete matching Silver records.
    def _soft_delete_by_keys(
        self,
        target_table: str,
        keys: Sequence[Sequence[Any]],  # keys is a list of key value lists,
        key_column: str,
        restriction_run_id: str,
        restriction_timestamp: datetime,
    ) -> int:

        target = DeltaTable.forName(self.spark, target_table)

        if not keys or not any(keys):
            return 0
        (
            target.update(
            condition=F.col(key_column).isin(keys),
            set={
                "silver_run_id": F.lit(restriction_run_id),
                "silver_updated_at": F.lit(restriction_timestamp),
                "silver_is_deleted": F.lit(True),
                "silver_deleted_at": F.lit(restriction_timestamp),
            },
            )
        )

        metrics = target.history(1).select("operationMetrics").first()

        if metrics is None or metrics["operationMetrics"] is None:
            return -1
        
        return int(metrics["operationMetrics"].get("numUpdatedRows", 0))


    ## Hard-delete matching Silver records.
    def _hard_delete_by_keys(
        self,
        target_table: str,
        keys: Sequence[Sequence[Any]],  # keys is a list of key value lists
        key_column: str,
    ) -> int:
        target = DeltaTable.forName(self.spark, target_table)

        if not keys or not any(keys):
            return 0        
        (
            target.delete(
            condition=F.col(key_column).isin(keys),            
            )
        )

        metrics = target.history(1).select("operationMetrics").first()
        
        if metrics is None or metrics["operationMetrics"] is None:
            return -1
        
        return int(metrics["operationMetrics"].get("numDeletedRows", 0))

    
    # -------------------------------------------------------------------------
    # Common helpers
    # -------------------------------------------------------------------------

    ## Build a Delta merge condition from key columns.
    # @staticmethod
    # def _merge_condition(join_column: str) -> str:
    #     if not join_column:
    #         raise ValueError("join_column cannot be empty")
    #     return f"t.`{join_column}` = s.`{join_column}`"


    ## Build one restriction processing log row.
    @staticmethod
    def _build_log(
        item: Dict[str, Any],
        context: RunContext,
        action: str,
        target_entity: str,
        started_at: datetime,
        ended_at: datetime,
        status: str,
        records_affected: int,
        error_message: str,
    ) -> Dict[str, Any]:
        return {
            "pipeline_run_id": context.pipeline_run_id,
            "restriction_run_id": context.run_id,
            "restriction_queue_id": item["restriction_queue_id"],
            "bronze_run_id": item["bronze_run_id"],
            "site_no": item["site_no"],
            "restriction_action": action,
            "target_entity": target_entity,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": int(
                (ended_at - started_at).total_seconds()
            ),
            "status": status,
            "records_affected": int(records_affected),
            "error_message": error_message,
        }
        

    ## Log a message if a logger is configured.
    def _log(
        self,
        level: str,
        message: str,
    ) -> None:
        if self.logger is None:
            return
        method = getattr(self.logger, level, None)
        if callable(method):
            method(message)
