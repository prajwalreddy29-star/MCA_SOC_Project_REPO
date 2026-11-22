from datetime import datetime, timezone
import json
import uuid

# Generate a unique run id. Example : bronze_cdp_incremental_20260527_143045
def generate_run_id(
    layer: str, 
    load_group: str, 
    load_phase: str
) -> str:

    return f"{layer}_{load_group}_{load_phase}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    
# Get the pipeline run id (jobRunId if launched by a job or a manual run with uuid)
def get_pipeline_run_id(dbutils_obj: object = None) -> str:

    try:
        context_json = dbutils_obj.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    except Exception:
        context_json = dbutils_obj.notebook.entry_point.getDbutils().notebook().getContext().safeToJson()  # For manual load with ServerLess
        
    context = json.loads(context_json)
    tags = context.get("tags", {}) or {}

    job_run_id = tags.get("jobRunId")
    if job_run_id:
        return job_run_id
    else:
        return f"manual_{uuid.uuid4().hex[:8]}"
    


## Runtime context shared across ingestion tasks.
## Create a new run context
# layer: The layer name, e.g. "bronze", "silver"
# load_group: Logical load group, e.g. "setup".
# load_phase: Logical load phase, e.g. "full"
class RunContext:
    
    def __init__(
            self,
            layer: str,
            load_group: str,
            load_phase: str,
            dbutils_obj: object = None,
            pipeline_run_id: str = "",
        ) -> None:

            self.pipeline_run_id = pipeline_run_id or get_pipeline_run_id(dbutils_obj)
            self.layer = layer
            self.load_group = load_group
            self.load_phase = load_phase
            self.run_id = generate_run_id(
                layer=layer,
                load_group=load_group,
                load_phase=load_phase,
            )
            self.started_at_utc = datetime.now(timezone.utc)

