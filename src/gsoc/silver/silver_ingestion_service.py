from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from generic.common.run_context import RunContext
from generic.common.control_tables import ControlTableHelper
from gsoc.bronze.bronze_reader import BronzeReader
from gsoc.silver.silver_writer import SilverWriter


# Silver ingestion service
# This service consumes Silver queue items and applies Bronze-to-Silver
# processing based on silver_processing_config.
#
# Design principles:
#    - Setup / full_replace tasks can be processed in parallel.
#    - Mutable entities are processed sequentially to avoid Delta conflicts.
#    - Queue and log tables are updated from the main thread only.
#    - Delta write logic is delegated to SilverWriter.
class SilverIngestionService:

    ## Initialize the Silver ingestion service.
    def __init__(
        self,
        control_helper: ControlTableHelper,
        bronze_reader: BronzeReader,
        silver_writer: SilverWriter,
        max_workers: int = 8,
    ) -> None:

        self.control_helper = control_helper
        self.bronze_reader = bronze_reader
        self.silver_writer = silver_writer
        self.max_workers = max_workers



    ## Run Silver ingestion for a given ingestion mode.
    # ingestion_mode examples:
    # - setup
    # - initial
    # - incremental
    def run(
        self,
        context: RunContext,
        ingestion_mode: str,
    ) -> Dict[str, Any]:

        work_items = self.control_helper.get_pending_silver_work_items(
            silver_run_id=context.run_id,
            ingestion_mode=ingestion_mode,
        )

        if not work_items:
            return {
                "pipeline_run_id": context.pipeline_run_id,
                "silver_run_id": context.run_id,
                "ingestion_mode": ingestion_mode,
                "total_tasks": 0,
                "success_count": 0,
                "failure_count": 0,
                "message": "No pending Silver work items found.",
            }

        if ingestion_mode == "full":
            results = self._run_setup_items(
                work_items=work_items,
                context=context,
            )
        else:
            results = self._run_cdp_items_by_entity(
                work_items=work_items,
                context=context,
            )

        self._finalize_results(
            results=results,
            context=context,
        )

        failure_count = sum(1 for r in results if r["status"] == "FAILED")
        success_count = sum(1 for r in results if r["status"] == "SUCCESS")
        skipped_count = sum(1 for r in results if r["status"] == "SKIPPED")

        return {
            "pipeline_run_id": context.pipeline_run_id,
            "silver_run_id": context.run_id,
            "ingestion_mode": ingestion_mode,
            "total_tasks": len(results),
            "success_count": success_count,
            "failure_count": failure_count,
            "skipped_count": skipped_count,
            "failures": [
                {
                    "queue_id": r["queue_id"],
                    "silver_task_id": r["silver_task_id"],
                    "error_message": r["error_message"],
                }
                for r in results
                if r["status"] == "FAILED"
            ],
        }


    ## Process setup Silver tasks in parallel.
    # Setup tables are independent and full-replace based.
    def _run_setup_items(
        self,
        work_items: List[Dict[str, Any]],
        context: RunContext,
    ) -> List[Dict[str, Any]]:

        results: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._process_silver_task,
                    item,
                    context,
                ): item
                for item in work_items
            }

            for future in as_completed(futures):
                results.append(future.result())

        return results


    ## Process CDP Silver tasks.
    # Tasks are grouped by entity_name.
    # Each entity is processed sequentially.
    # Different entities can run in parallel.
    def _run_cdp_items_by_entity(
        self,
        work_items: List[Dict[str, Any]],
        context: RunContext,
    ) -> List[Dict[str, Any]]:

        grouped_items = self._group_items_by_entity(work_items)

        all_results: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._process_entity_group,
                    entity_name,
                    entity_items,
                    context,
                ): entity_name
                for entity_name, entity_items in grouped_items.items()
            }

            for future in as_completed(futures):
                all_results.extend(future.result())

        return all_results


    ## Group work items by entity_name.
    def _group_items_by_entity(
        self,
        work_items: List[Dict[str, Any]],
    ) -> Dict[str, List[Dict[str, Any]]]:

        grouped = defaultdict(list)

        for item in work_items:
            entity_name = item.get("entity_name") or item.get("silver_task_id")
            grouped[entity_name].append(item)

        return dict(grouped)


    ## Process all tasks for one entity sequentially.
    # Example:
    # - sites snapshot/change/delete
    # - systems snapshot/change/delete
    #
    # If one task fails, remaining tasks for the same entity are not processed.
    # They remain PENDING and will be retried in a future run.
    def _process_entity_group(
        self,
        entity_name: str,
        entity_items: List[Dict[str, Any]],
        context: RunContext,
    ) -> List[Dict[str, Any]]:

        results: List[Dict[str, Any]] = []

        ordered_items = sorted(
            entity_items,
            key=lambda x: (
                x.get("processing_order") if x.get("processing_order") is not None else 999,
                x.get("queue_created_at"),
            ),
        )

        for item in ordered_items:
            result = self._process_silver_task(
                item=item,
                context=context,
            )

            results.append(result)

            if result["status"] == "FAILED":
                # Stop processing this entity.
                # Remaining items are not marked as failed.
                # They stay PENDING in the queue and will be retried later.
                break

        return results


    ## Process one Silver work item based on processing_pattern.
    def _process_silver_task(
        self,
        item: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:

        started_at = datetime.now(timezone.utc)

        try:
            processing_pattern = item["processing_pattern"]

            if processing_pattern == "replace":
                writer_result = self._process_full_replace(
                    item=item,
                    context=context,
                )

            elif processing_pattern == "append":
                writer_result = self._process_append(
                    item=item,
                    context=context,
                )

            elif processing_pattern in ("merge_scd1"):
                writer_result = self._process_merge_scd1(
                    item=item,
                    context=context,
                )
            
            elif processing_pattern == "merge_changes":
                writer_result = self._process_merge_changes(
                    item=item,
                    context=context,
                )

            elif processing_pattern == "delete":
                writer_result = self._process_soft_delete(
                    item=item,
                    context=context,
                )

            else:
                raise ValueError(
                    f"Unsupported processing_pattern={processing_pattern} "
                    f"for silver_task_id={item.get('silver_task_id')}"
                )

            ended_at = datetime.now(timezone.utc)

            records_written = writer_result.get("records_written", 0)

            return {
                "pipeline_run_id": context.pipeline_run_id,
                "silver_run_id": context.run_id,
                "queue_id": item["queue_id"],                
                "silver_task_id": item["silver_task_id"],                                
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_seconds": int((ended_at - started_at).total_seconds()),
                "status": "SUCCESS",
                "records_read": records_written,
                "records_inserted": records_written,
                "error_message": "",
            }

        except Exception as exc:
            ended_at = datetime.now(timezone.utc)

            return {
                "pipeline_run_id": context.pipeline_run_id,
                "silver_run_id": context.run_id,
                "queue_id": item.get("queue_id"),                
                "silver_task_id": item.get("silver_task_id"),                
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_seconds": int((ended_at - started_at).total_seconds()),
                "status": "FAILED",
                "records_read": -1,
                "records_inserted": -1,
                "error_message": str(exc),
            }


    ## Full replace processing.
    # Typical use case:
    # - Setup/reference tables.
    def _process_full_replace(
        self,
        item: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:

        df = self.bronze_reader.read_for_run(
            table_name=item["source_bronze_table"],
            bronze_run_id=item["bronze_run_id"],
        )


        return self.silver_writer.full_replace(
            data=df,
            table_name=item["target_silver_table"],
            silver_run_id=context.run_id,
            allow_new_columns=True,
        )


    ## Append-only processing.
    # Typical use case:
    # - EventHistory.
    def _process_append(
        self,
        item: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:

        key_columns = item.get("primary_key_columns")
        
        if not key_columns:
            raise ValueError(
                f"Missing primary_key_columns for silver_task_id={item.get('silver_task_id')}"
            )

        # 1. Read Bronze data for the current run
        df = self.bronze_reader.read_for_run(
            table_name=item["source_bronze_table"],
            bronze_run_id=item["bronze_run_id"],
        )

        #2. Retrieve last successful Silver run id for the same entity
        max_silver_run_id = self.control_helper.get_last_success_silver_run_id_for_entity(
            entity_name=item["entity_name"]
        )

        return self.silver_writer.append(
            data=df,
            table_name=item["target_silver_table"],
            silver_run_id=context.run_id,
            key_columns=key_columns,
            max_silver_run_id=max_silver_run_id
        )


    ## SCD1 merge processing.
    # Typical use case:
    # - Initial snapshot into mutable Silver table.
    # - Change endpoint into mutable Silver table.
    def _process_merge_scd1(
        self,
        item: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:

        #key_columns = self._parse_csv_columns(item.get("primary_key_columns"))
        key_columns = item.get("primary_key_columns")
        
        if not key_columns:
            raise ValueError(
                f"Missing primary_key_columns for silver_task_id={item.get('silver_task_id')}"
            )

        df = self.bronze_reader.read_for_run(
            table_name=item["source_bronze_table"],
            bronze_run_id=item["bronze_run_id"],
        )

        return self.silver_writer.merge_scd1(
            data=df,
            table_name=item["target_silver_table"],
            silver_run_id=context.run_id,
            key_columns=key_columns,
        )

    
    ## SSAE SOC merge processing.
    # Typical use case:
    # - TranType = A => Addition => Insert
    # - TranType = C => Change => Update  or Insert => !! We can receive a Change "C" without an Addition "A" before. So if the record doesn't exist we need to create it.
    def _process_merge_changes(
        self,
        item: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:

        #key_columns = self._parse_csv_columns(item.get("primary_key_columns"))
        key_columns = item.get("primary_key_columns")

        if not key_columns:
            raise ValueError(
                f"Missing primary_key_columns for merge_changes "
                f"silver_task_id={item.get('silver_task_id')}"
            )

        df = self.bronze_reader.read_for_run(
            table_name=item["source_bronze_table"],
            bronze_run_id=item["bronze_run_id"],
        )

        return self.silver_writer.merge_changes(
            data=df,
            table_name=item["target_silver_table"],
            silver_run_id=context.run_id,
            key_columns=key_columns,
            tran_type_column="tran_type",
        )

    
    ## Soft delete processing.
    # Typical use case:
    # - Site deletes.
    # - System deletes.
    def _process_soft_delete(
        self,
        item: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:

        #key_columns = self._parse_csv_columns(item.get("primary_key_columns"))
        key_columns = item.get("primary_key_columns")

        if not key_columns:
            raise ValueError(
                f"Missing primary_key_columns for silver_task_id={item.get('silver_task_id')}"
            )

        df = self.bronze_reader.read_for_run(
            table_name=item["source_bronze_table"],
            bronze_run_id=item["bronze_run_id"],
        )

        return self.silver_writer.soft_delete(
            data=df,
            table_name=item["target_silver_table"],
            silver_run_id=context.run_id,
            key_columns=key_columns,
        )


    ## Write logs and update queue statuses after processing.
    def _finalize_results(
        self,
        results: List[Dict[str, Any]],
        context: RunContext,
    ) -> None:

        if not results:
            return

        success_queue_ids = [
            r["queue_id"]
            for r in results
            if r["status"] == "SUCCESS"
        ]

        failed_results = [
            r
            for r in results
            if r["status"] == "FAILED"
        ]

        failed_queue_ids = [
            r["queue_id"]
            for r in failed_results
            if r.get("queue_id")
        ]

        if success_queue_ids:
            self.control_helper.update_silver_queue_status(
                queue_ids=success_queue_ids,
                status="SUCCESS",
                silver_run_id=context.run_id,
                error_message=None
            )

        if failed_queue_ids:
            # Failed items are returned to PENDING so they can be retried.
            self.control_helper.update_silver_queue_status(
                queue_ids=failed_queue_ids,
                status="PENDING",
                silver_run_id=context.run_id,
                error_message="Silver processing failed. See Silver run log for details."
            )

                
        self.control_helper.append_run_logs(results)

    
    '''
    ## Parse comma-separated column list from config.
    # Example:
    # - "country_code,site_no"
    @staticmethod
    def _parse_csv_columns(
        value: Optional[str],
    ) -> List[str]:

        if not value:
            return []

        return [
            col.strip()
            for col in value.split(",")
            if col.strip()
        ]
    '''
