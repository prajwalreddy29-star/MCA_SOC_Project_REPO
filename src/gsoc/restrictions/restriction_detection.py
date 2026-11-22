from typing import Any, Dict, List, Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from generic.common.run_context import RunContext
from generic.common.control_tables import ControlTableHelper


## Detect restriction changes from Bronze and enqueue restriction-specific actions.
# This service does not execute the restriction action itself.
# It only creates items in restriction_processing_queue.
class RestrictionDetectionService:

    def __init__(
        self,
        spark: SparkSession,
        control_helper: ControlTableHelper,
        bronze_run_id_column: str = "bronze_run_id"
    ) -> None:

        self.spark = spark
        self.control_helper = control_helper
        self.bronze_run_id_column = bronze_run_id_column


    ## Detect restriction actions for one successful Bronze restriction_changes endpoint.
    # Expected input item:
    # {
    #   "pipeline_run_id": "...",
    #   "bronze_run_id": "...",
    #   "endpoint_id": "...",
    #   "target_bronze_table": "..."
    # }
    def detect_for_bronze_item(
        self,
        bronze_item: Dict[str, Any],
    ) -> Dict[str, Any]:

        pipeline_run_id = bronze_item.get("pipeline_run_id")
        bronze_run_id = bronze_item["bronze_run_id"]
        source_endpoint_id = bronze_item["endpoint_id"]
        source_bronze_table = bronze_item["target_bronze_table"]

        df = (
            self.spark.table(source_bronze_table)
            .filter(F.col(self.bronze_run_id_column) == F.lit(bronze_run_id))
        )

        row_count = df.count()

        if df.take(1) == []:
            return {
                "bronze_run_id": bronze_run_id,
                "source_endpoint_id": source_endpoint_id,
                "actions_detected": 0,
                "status": "SUCCESS",
                "message": "No restriction changes found for this bronze_run_id.",
            }

        actions_df = (
            df
            .withColumn(
                "restriction_action",
                F.when(
                    F.col("excluded") == F.lit(False),
                    F.lit("INCLUDE")
                )
                .when(
                    F.col("excluded") == F.lit(True),
                    F.lit("EXCLUDE")
                )
                .otherwise(F.lit(None))
            )
            .filter(F.col("restriction_action").isNotNull())
            .select(
                F.lit(pipeline_run_id).alias("pipeline_run_id"),
                F.lit(bronze_run_id).alias("bronze_run_id"),
                F.col("site_no"),
                F.col("restriction_action"),
                F.lit(source_endpoint_id).alias("source_endpoint_id"),
            )
            .dropDuplicates([
                "bronze_run_id",
                "site_no",
                "restriction_action",
                "source_endpoint_id",
            ])
        )

        rows = [
            row.asDict(recursive=True)
            for row in actions_df.collect()
        ]

        self.control_helper.enqueue_restriction_tasks(rows)

        return {
            "bronze_run_id": bronze_run_id,
            "source_endpoint_id": source_endpoint_id,
            "actions_detected": len(rows),
            "status": "SUCCESS",
            "message": "Restriction actions enqueued.",
        }


    ## Detect restriction actions for multiple successful Bronze items.
    def detect_for_multiple_bronze_items(
        self,
        bronze_items: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:

        results = []

        for item in bronze_items:
            try:
                result = self.detect_for_bronze_item(item)
                results.append(result)
            except Exception as exc:
                results.append({
                    "bronze_run_id": item.get("bronze_run_id"),
                    "source_endpoint_id": item.get("endpoint_id"),
                    "actions_detected": -1,
                    "status": "FAILED",
                    "message": str(exc),
                })

        return results


