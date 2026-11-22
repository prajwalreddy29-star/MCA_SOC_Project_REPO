from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from generic.common.run_context import RunContext
from generic.common.control_tables import ControlTableHelper
from gsoc.bronze.bronze_writer import BronzeWriter
from generic.ssae_api.ssae_client import SsaeApiClient


# Generate consecutive [from_dt, to_dt] windows based on window_days.
# Example: build_date_windows(start_dt=2026-01-01, end_dt=2026-01-05, window_days=2) -> [(2026-01-01, 2026-01-03), (2026-01-03, 2026-01-05)]
def build_date_windows(
    start_dt: datetime,
    end_dt: datetime,
    window_days: int,
) -> List[Tuple[datetime, datetime]]:

    if window_days <= 0:
        raise ValueError("window_days must be greater than 0")

    windows = []
    current_from = start_dt

    while current_from < end_dt:
        current_to = min(current_from + timedelta(days=window_days), end_dt)
        windows.append((current_from, current_to))
        current_from = current_to

    return windows


# Convert datetime to UTC ISO string.
def to_iso_utc(dt: datetime) -> str:

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc).isoformat()



## Initial ingestion service for SSAE CDP Operational and Transactional endpoints.
# - Load initial/snapshot endpoints.
# - Write raw data to Bronze in append mode.
# - Mark endpoint initial load as completed after successful Bronze write.
# - Append structured run logs in batches
class CdpInitialIngestionService:

    def __init__(
        self,
        client,
        bronze_writer,
        control_helper,
        max_workers: int = 8
    ) -> None:
        self.client = client
        self.bronze_writer = bronze_writer
        self.control_helper = control_helper
        self.max_workers = max_workers        

    
    
    ## Run cdp initial load ingestion.
    # This method always processes all configured endpoints.
    # countries: Optional list of countries to process.
    # Returns: Summary dictionary.
    # TODO ? : Add force_reload flag to force reload all endpoints.
    
    def run(
        self, 
        context: RunContext,
        countries: Optional[List[str]] = None,
        mode: str = "initial"
    ) -> Dict[str, Any]:

        if mode == "initial":  
            endpoint_configs = self.control_helper.get_active_cdp_initial_endpoints_never_loaded(
            countries = countries,
            )
        #elif mode == "recover":
        #    endpoint_configs = self.control_helper.get_active_cdp_endpoints(
        #    countries=countries,
        #    ingestion_mode = "initial"
        #    )        
        else:
            raise ValueError(f"Invalid mode: {mode}")

        all_results: List[Dict[str, Any]] = []
        pending_logs: List[Dict[str, Any]] = []
        pending_states: List[Dict[str, Any]] = []
        pending_init_states: List[Dict[str, Any]] = []
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
                        "initial_load_completed" : True,
                        "last_status": result["status"],
                        "last_success_bronze_run_id": result["bronze_run_id"],
                        "last_success_at": result["ended_at"],
                        "last_from_datetime": result.get("from_datetime") if "from_datetime" in result else None,
                        "last_to_datetime": result.get("to_datetime") if "to_datetime" in result else None,
                        "last_records_fetched": result["records_fetched"],
                        "last_records_written": result["records_written"],
                        "last_error_message": result["error_message"]
                    })

                    pending_init_states.append({                        
                        "entity_name" : cfg.get("entity_name"),
                        "country_code" : cfg.get("country_code"),
                        "last_to_datetime" : result.get("to_datetime")  #Init last_to_datetime for all other endpoints of this entity_name to prepare next incremental load
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
                
                
        if pending_logs:
            self.control_helper.append_run_logs(pending_logs)

        if pending_states:
            self.control_helper.update_run_states(pending_states)

        if pending_init_states:
            self.control_helper.init_incremental_states_batch(pending_init_states)

        if pending_queue_items:
            self.control_helper.enqueue_silver_tasks_batch(pending_queue_items)
        
        failure_count = sum(1 for r in all_results if r["status"] == "FAILED")
        success_count = len(all_results) - failure_count

        summary = {
            "pipeline_run_id": context.pipeline_run_id,
            "bronze_run_id": context.run_id,
            "total_endpoints": len(all_results),
            "success_count": success_count,
            "failure_count": failure_count,
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

    
    ## Run initial full load for one country and one cdp table.
    def _process_endpoint(
        self,
        cfg: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:
        
        load_pattern = cfg.get("load_pattern")

        if load_pattern == "snapshot":
            return self._process_snapshot_endpoint(cfg, context)

        if load_pattern == "historical":
            return self._process_historical_endpoint(cfg, context)

        raise ValueError(
            f"Unsupported load_pattern={load_pattern} for endpoint_id={cfg.get('endpoint_id')}"
        )



    ## Run initial full load for one country and one cdp table for "snapshot" endpoints.
    def _process_snapshot_endpoint(
        self,
        cfg: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:

        started_at = datetime.now(timezone.utc)

        try:
            
            records = self._fetch_records(cfg)
            records_fetched = len(records)

            write_result = self.bronze_writer.write(
                data=records,
                table_name=cfg["target_bronze_table"],
                bronze_run_id=context.run_id,
                mode="append"
            )

            ended_at = datetime.now(timezone.utc)

            return {
                "pipeline_run_id": context.pipeline_run_id,
                "bronze_run_id": context.run_id,
                "endpoint_id": cfg["endpoint_id"],
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_seconds": int((ended_at - started_at).total_seconds()),
                "status": "SUCCESS",
                "to_datetime" : started_at,
                "records_fetched": records_fetched,
                "records_written": write_result.get("records_written", records_fetched),
                "error_message": "",                
            }

        except Exception as exc:
            ended_at = datetime.now(timezone.utc)

            return {
                "pipeline_run_id": context.pipeline_run_id,
                "bronze_run_id": context.run_id,
                "endpoint_id": cfg["endpoint_id"],                
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_seconds": int((ended_at - started_at).total_seconds()),
                "status": "FAILED",
                "records_fetched": -1,
                "records_written": -1,
                "error_message": str(exc),
            }


    ## Run initial full load for one country and one cdp table for "historical" endpoints.        
    def _process_historical_endpoint(
        self,
        cfg: Dict[str, Any],
        context: RunContext,
    ) -> Dict[str, Any]:

        started_at = datetime.now(timezone.utc)

        lookback_days = cfg.get("initial_lookback_days")
        window_days = cfg.get("initial_window_days")

        if lookback_days is None:
            raise ValueError(f"Missing initial_lookback_days for {cfg['endpoint_id']}")

        if window_days is None:
            raise ValueError(f"Missing initial_window_days for {cfg['endpoint_id']}")

        now_utc = datetime.now(timezone.utc)
        initial_from = now_utc - timedelta(days=int(lookback_days))
        initial_to = now_utc

        total_fetched = 0
        total_written = 0
        window_count = 0

        try:

            for window_from, window_to in build_date_windows(
                start_dt=initial_from,
                end_dt=initial_to,
                window_days=int(window_days),
            ):
                window_count += 1

                from_date = to_iso_utc(window_from)
                to_date = to_iso_utc(window_to)

                records = self._fetch_records(
                    cfg=cfg,
                    from_date=from_date,
                    to_date=to_date,
                )

                records_fetched = len(records)

                write_result = self.bronze_writer.write(
                    data=records,
                    table_name=cfg["target_bronze_table"],
                    bronze_run_id=context.run_id,
                    mode="append"
                )
                
                total_fetched += records_fetched
                total_written += write_result.get("records_written", records_fetched)

                # update state after each successful window
                self.control_helper.update_endpoints_state(
                    endpoint_id = cfg["endpoint_id"],
                    last_from_datetime = from_date,
                    last_to_datetime = to_date,
                    last_records_fetched = records_fetched,
                    last_records_written = write_result.get("records_written", records_fetched)
                )

            ended_at = datetime.now(timezone.utc)

            return {
                    "pipeline_run_id": context.pipeline_run_id,
                    "bronze_run_id": context.run_id,
                    "endpoint_id": cfg["endpoint_id"],
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "from_datetime": initial_from,
                    "to_datetime": initial_to,                    
                    "duration_seconds": int((ended_at - started_at).total_seconds()),
                    "status": "SUCCESS",
                    "records_fetched": total_fetched,
                    "records_written": total_written,
                    "error_message": "",                
                }
        
        except Exception as exc:
            ended_at = datetime.now(timezone.utc)

            return {
                "pipeline_run_id": context.pipeline_run_id,
                "bronze_run_id": context.run_id,
                "endpoint_id": cfg["endpoint_id"],                
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_seconds": int((ended_at - started_at).total_seconds()),
                "status": "FAILED",
                "records_fetched": total_fetched,
                "records_written": total_written,
                "error_message": str(exc),
            }



    ## Fetch cdp records using the specific method defined in endpoint_config.ssae_api_class_method
    def _fetch_records(
        self,
        cfg: Dict[str, Any],
        from_date: str = None,
        to_date: str = None,
    ) -> List[Dict[str, Any]]:
        
        load_pattern = cfg.get("load_pattern")
        country = cfg["api_country_code"]
        fetch_size = cfg.get("max_fetch_size")

        method_name = cfg["ssae_api_class_method"]

        if not hasattr(self.client, method_name):
            raise ValueError(f"SSAE client method not found: {method_name}")

        method = getattr(self.client, method_name)

        if load_pattern == "snapshot":
            return method(
                country=country,
                page_size = fetch_size
            )

        if load_pattern == "historical":
            return method(
                country=country,
                from_date=from_date,
                to_date=to_date,
                page_size = fetch_size
            )
            
