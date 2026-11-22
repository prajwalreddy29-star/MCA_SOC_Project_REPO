from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from generic.common.run_context import RunContext
from generic.common.control_tables import ControlTableHelper
from gsoc.bronze.bronze_writer import BronzeWriter
from generic.ssae_api.ssae_client import SsaeApiClient

## Ingestion service for SSAE setup/reference tables.
# - Read setup endpoint configuration.
# - Fetch setup data from SSAE.
# - Write setup data to Bronze in overwrite mode.
# - Log success/failure per endpoint.
class SetupIngestionService:

    def __init__(
        self,
        client: SsaeApiClient,
        bronze_writer: BronzeWriter,
        control_helper: ControlTableHelper,
        max_workers: int = 8,
        logger: Optional[Any] = None,
    ) -> None:
        self.client = client
        self.bronze_writer = bronze_writer
        self.control_helper = control_helper
        self.max_workers = max_workers
        self.logger = logger


    ## Run setup full load ingestion.
    # This method always processes all configured endpoints.
    # countries: Optional list of countries to process.
    # Returns: Summary dictionary.
    def run(
        self,
        context: RunContext,
        countries: Optional[List[str]] = None,
    ) -> Dict[str, Any]:

        endpoint_configs = self.control_helper.get_active_setup_endpoints(
            countries=countries,
        )
                
        all_results: List[Dict[str, Any]] = []
        pending_logs: List[Dict[str, Any]] = []
        pending_states: List[Dict[str, Any]] = []
        pending_queue_items: List[Dict[str, Any]] = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._run_one_endpoint,
                    cfg,
                    context,
                ): cfg
                for cfg in endpoint_configs
            }

            for future in as_completed(futures):
                result = future.result()

                all_results.append(result)
                pending_logs.append(result)

                
                if result["status"] == "SUCCESS":
                    pending_queue_items.append({
                        "bronze_run_id": result["bronze_run_id"],
                        "endpoint_id": result["endpoint_id"],
                    })


                state = {
                    "endpoint_id": result["endpoint_id"],
                    "last_status": result["status"],
                    "last_records_fetched": result["records_fetched"],
                    "last_records_written": result["records_written"],
                    "last_error_message": result["error_message"]
                }
                if result["status"] == "SUCCESS":
                    state["last_success_bronze_run_id"] = result["bronze_run_id"]
                    state["last_success_at"] = result["ended_at"]

                pending_states.append(state)

                
        # Final flush
        # One single write after all parallel workers are done
        if pending_logs:
            self.control_helper.append_run_logs(pending_logs)

        if pending_states:
            self.control_helper.update_run_states(pending_states)
      
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

    ## Run setup full load for one country and one setup table.
    def _run_one_endpoint(
        self,
        cfg: Dict[str, Any],
        context: RunContext,
    ) -> None:

        started_at = datetime.now(timezone.utc)
        
        try:
            records = self._fetch_setup_records(cfg)
            
            records_fetched = len(records)

            write_result = self.bronze_writer.write(
                data=records,
                table_name=cfg["target_bronze_table"],
                bronze_run_id=context.run_id,
                mode="overwrite",
                allow_new_columns=True
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

    ## Fetch setup records using the generic setup table method.
    def _fetch_setup_records(
        self,
        cfg: Dict[str, Any],
    ) -> List[Dict[str, Any]]:

        api_country_code = cfg["api_country_code"]
        endpoint_name = cfg["endpoint_name"]

        return self.client.get_setup_table(
            country=api_country_code,
            table_name=endpoint_name,
        )

    ## Log a message if a logger is configured.
    def _log(self, level: str, message: str) -> None:
        if self.logger is None:
            return

        log_method = getattr(self.logger, level, None)

        if callable(log_method):
            log_method(message)