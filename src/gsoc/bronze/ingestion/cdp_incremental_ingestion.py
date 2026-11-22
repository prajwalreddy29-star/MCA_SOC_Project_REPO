from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from generic.common.run_context import RunContext
from generic.common.control_tables import ControlTableHelper
from gsoc.bronze.bronze_writer import BronzeWriter
from generic.ssae_api.ssae_client import SsaeApiClient
from gsoc.restrictions.restriction_detection import RestrictionDetectionService

# Convert datetime to UTC ISO string.
def to_iso_utc(dt: datetime) -> str:

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")



# Parse ISO datetime string to timezone-aware UTC datetime.
def parse_iso_utc(value: Any) -> datetime:

    if isinstance(value, datetime):
        dt = value
    else:
        value = str(value)
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


## Incremental ingestion service for SSAE CDP Operational and Transactional endpoints.
# - Load incremental endpoints.
# - Supports date-window based endpoints and cursor-based EventHistory.
# - Write raw data to Bronze in append mode.
# - Update endpoint state only after successful Bronze write.
# - Append structured run logs in batches.
class CdpIncrementalIngestionService:

    def __init__(
        self,
        client,
        bronze_writer,
        control_helper,
        restriction_detector: Optional[RestrictionDetectionService] = None,
        max_workers: int = 8,
        overlap_seconds: int = 10,
    ) -> None:
        self.client = client
        self.bronze_writer = bronze_writer
        self.control_helper = control_helper
        self.restriction_detector = restriction_detector
        self.max_workers = max_workers
        self.overlap_seconds = overlap_seconds

    ## Run CDP incremental load ingestion.
    # This method processes all configured incremental endpoints.
    # countries: Optional list of countries to process.
    # Returns: Summary dictionary.
    def run(
        self,
        context: RunContext,
        countries: Optional[List[str]] = None,
    ) -> Dict[str, Any]:

        endpoint_configs = self.control_helper.get_active_cdp_endpoints(
            countries = countries,
            ingestion_mode = "incremental"
        )

        all_results: List[Dict[str, Any]] = []
        pending_logs: List[Dict[str, Any]] = []
        pending_states: List[Dict[str, Any]] = []
        pending_queue_items: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._process_endpoint,
                    cfg,
                    context,
                ): cfg
                for cfg in endpoint_configs
            }

            for future in as_completed(futures):
                result = future.result()
                cfg = futures[future]
                all_results.append(result)
                pending_logs.append(result)

                # Update state and move watermark/cursor only if the endpoint was successfully fetched and written to Bronze.
                if result["status"] == "SUCCESS":
                    pending_states.append({
                        "endpoint_id": result["endpoint_id"],
                        "last_status": result["status"],
                        "last_success_bronze_run_id": result["bronze_run_id"],
                        "last_success_at": result["ended_at"],
                        "last_cursor": result["output_cursor"] if "output_cursor" in result else None,
                        "last_from_datetime": result.get("from_datetime") if "from_datetime" in result else None,
                        "last_to_datetime": result.get("to_datetime") if "to_datetime" in result else None,
                        "last_records_fetched": result["records_fetched"],
                        "last_records_written": result["records_written"],
                        "last_error_message": result["error_message"]
                    })

                    if result["records_written"] > 0:
                        pending_queue_items.append({
                            "bronze_run_id": result["bronze_run_id"],
                            "endpoint_id": result["endpoint_id"],
                        })

                else:
                    # Store failure state without moving the watermark/cursor.
                    pending_states.append({
                        "endpoint_id": result["endpoint_id"],
                        "last_status": result["status"],                        
                        "last_records_fetched": result["records_fetched"],
                        "last_records_written": result["records_written"],
                        "last_error_message": result["error_message"]
                    })
        
        if pending_states:
            self.control_helper.update_run_states(pending_states)

        if pending_queue_items:
            self.control_helper.enqueue_silver_tasks_batch(pending_queue_items)

        failure_count = sum(1 for r in all_results if r["status"] == "FAILED")
        success_count = len(all_results) - failure_count

        restriction_success_items = [
            {
                "pipeline_run_id": r["pipeline_run_id"],
                "bronze_run_id": r["bronze_run_id"],
                "endpoint_id": r["endpoint_id"],
                "target_bronze_table": r["target_bronze_table"]
            }
            for r in all_results
            if r["status"] == "SUCCESS"            
            and r.get("endpoint_name") == "restrictions_changes"
            and r.get("records_written", 0) > 0
        ]
        
        if restriction_success_items:
            self.restriction_detector.detect_for_multiple_bronze_items(bronze_items=restriction_success_items)

        if pending_logs:
            self.control_helper.append_run_logs(pending_logs)
            

        summary = {
            "pipeline_run_id": context.pipeline_run_id,
            "bronze_run_id": context.run_id,
            "total_endpoints": len(all_results),
            "success_count": success_count,
            "failure_count": failure_count,
            "restriction_detection": len(restriction_success_items),
            "failures": [
                {
                    "endpoint_id": r["endpoint_id"],
                    "error_message": r["error_message"],
                }
                for r in all_results
                if r["status"] == "FAILED"
            ],
        }

        return summary

    ## Run incremental load for one configured endpoint.
    def _process_endpoint(
        self,
        cfg: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:

        load_pattern = cfg.get("load_pattern")

        if load_pattern == "dates_based":
            return self._process_incremental_dates_endpoint(cfg, context)

        if load_pattern == "cursor_based":
            return self._process_incremental_cursor_endpoint(cfg, context)

        raise ValueError(
            f"Unsupported load_pattern={load_pattern} "
            f"for endpoint_id={cfg.get('endpoint_id')}"
        )

    
    ## Run incremental load for dates_based endpoint.
    # Examples:
    # - sites/changes
    # - sites/deletes
    def _process_incremental_dates_endpoint(
        self,
        cfg: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:

        started_at = datetime.now(timezone.utc)

        try:
            endpoint_id = cfg["endpoint_id"]

            state = self.control_helper.get_endpoint_state(
                endpoint_id=endpoint_id,
            )

            # Get the last_to_datetime that will become the from_datetime to fetch new records.
            previous_watermark = state.get("last_to_datetime")

            if previous_watermark is None:
                raise ValueError(
                    f"Missing last_to_datetime for endpoint_id={endpoint_id}. "
                    "Initial load must initialize incremental state before running incremental load."
                )

            previous_to_dt = parse_iso_utc(previous_watermark)

            # Add a small overlap to avoid missing boundary records.
            from_dt = previous_to_dt - timedelta(seconds=self.overlap_seconds)
            to_dt = datetime.now(timezone.utc)

            from_date = to_iso_utc(from_dt)
            to_date = to_iso_utc(to_dt)

            records = self._fetch_datesbased_records(
                cfg=cfg,
                from_date=from_date,
                to_date=to_date,
            )

            records_fetched = len(records)
            
            if records_fetched > 0:
                write_result = self.bronze_writer.write(
                    data=records,
                    table_name=cfg["target_bronze_table"],
                    bronze_run_id=context.run_id,
                    mode="append"
                )
    
                records_written = write_result.get("records_written", records_fetched)

            ended_at = datetime.now(timezone.utc)

            return {
                "pipeline_run_id": context.pipeline_run_id,
                "bronze_run_id": context.run_id,
                "endpoint_id": cfg["endpoint_id"],
                "endpoint_name": cfg["endpoint_name"],
                "target_bronze_table": cfg["target_bronze_table"],
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_seconds": int((ended_at - started_at).total_seconds()),
                "status": "SUCCESS",
                "from_datetime": from_dt,
                "to_datetime": to_dt,
                "records_fetched": records_fetched,
                "records_written": records_written if records_fetched > 0 else 0,
                "error_message": "",
            }

        except Exception as exc:
            ended_at = datetime.now(timezone.utc)

            return {
                "pipeline_run_id": context.pipeline_run_id,
                "bronze_run_id": context.run_id,
                "endpoint_id": cfg["endpoint_id"],
                "endpoint_name": cfg["endpoint_name"],
                "target_bronze_table": cfg["target_bronze_table"],
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_seconds": int((ended_at - started_at).total_seconds()),
                "status": "FAILED",
                "records_fetched": -1,
                "records_written": -1,
                "error_message": str(exc),
            }


    ## Run incremental load for one cursor-based endpoint.
    # Expected use case:
    # - eventHistories/changes/{cursor}
    def _process_incremental_cursor_endpoint(
        self,
        cfg: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:

        started_at = datetime.now(timezone.utc)
        cursor = None
        next_cursor = None
        
        try:
            endpoint_id = cfg["endpoint_id"]

            state = self.control_helper.get_endpoint_state(
                endpoint_id=endpoint_id,
            )
            
            cursor = state.get("last_cursor")

            if not cursor:
                # After init. there is no cursor. needs to get cursor with last_date
                previous_watermark = state.get("last_to_datetime")

                if previous_watermark is None:
                    raise ValueError(
                        f"Missing last_to_datetime for endpoint_id={endpoint_id}. "
                        "Initial load must initialize incremental state before running incremental load."
                    )
    
                previous_to_dt = parse_iso_utc(previous_watermark)
                # Add a small overlap to avoid missing boundary records.
                to_dt = previous_to_dt - timedelta(seconds=self.overlap_seconds)
                to_date = to_iso_utc(to_dt)

                cursor = self.client.get_event_history_cursor(
                    country=cfg["api_country_code"],
                    to_date=to_date
                )

            records, next_cursor = self._fetch_cursorbased_records(
                cfg=cfg,
                cursor=cursor,
            )

            records_fetched = len(records)
            
            if records_fetched > 0:
                write_result = self.bronze_writer.write(
                    data=records,
                    table_name=cfg["target_bronze_table"],
                    bronze_run_id=context.run_id,
                    mode="append"
                )
    
                records_written = write_result.get("records_written", records_fetched)

            ended_at = datetime.now(timezone.utc)
            
            return {
                "pipeline_run_id": context.pipeline_run_id,
                "bronze_run_id": context.run_id,
                "endpoint_id": cfg["endpoint_id"],
                "endpoint_name": cfg["endpoint_name"],
                "target_bronze_table": cfg["target_bronze_table"],
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_seconds": int((ended_at - started_at).total_seconds()),
                "status": "SUCCESS",
                "from_datetime": started_at,
                "to_datetime": started_at,                
                "input_cursor": cursor if cursor else "",
                "output_cursor": next_cursor if next_cursor else "",
                "records_fetched": records_fetched,
                "records_written": records_written if records_fetched > 0 else 0,
                "error_message": "",
            }

        except Exception as exc:
            ended_at = datetime.now(timezone.utc)

            return {
                "pipeline_run_id": context.pipeline_run_id,
                "bronze_run_id": context.run_id,
                "endpoint_id": cfg["endpoint_id"],
                "endpoint_name": cfg["endpoint_name"],
                "target_bronze_table": cfg["target_bronze_table"],
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_seconds": int((ended_at - started_at).total_seconds()),
                "status": "FAILED",
                "input_cursor": cursor if cursor else "",
                "records_fetched": -1,
                "records_written": -1,
                "error_message": str(exc),
            }

    ## Fetch records for a date-window incremental endpoint.
    def _fetch_datesbased_records(
        self,
        cfg: Dict[str, Any],
        from_date: str,
        to_date: str,
    ) -> List[Dict[str, Any]]:

        country = cfg["api_country_code"]
        fetch_size = cfg.get("max_fetch_size")
        method_name = cfg["ssae_api_class_method"]

        if not hasattr(self.client, method_name):
            raise ValueError(f"SSAE client method not found: {method_name}")

        method = getattr(self.client, method_name)

        records_iterable = method(
            country=country,
            from_date=from_date,
            to_date=to_date,
            fetch=fetch_size,
        )

        # Client methods return Iterable for CDP endpoints.
        return list(records_iterable)

    ## Fetch records for cursor-based incremental endpoint.
    def _fetch_cursorbased_records(
        self,
        cfg: Dict[str, Any],
        cursor: str,
    ) -> tuple:

        country = cfg["api_country_code"]
        method_name = cfg["ssae_api_class_method"]

        if not hasattr(self.client, method_name):
            raise ValueError(f"SSAE client method not found: {method_name}")

        method = getattr(self.client, method_name)

        records, next_cursor = method(
            country=country,
            cursor=cursor,
        )

        if next_cursor is None:
            raise ValueError(
                f"Cursor endpoint did not return next_cursor for endpoint_id={cfg['endpoint_id']}"
            )

        return list(records), next_cursor
    