# notebooks/config/environment.py
import json
from typing import Optional

ENVIRONMENT_TAG = "global.operation.environment"
SUPPORTED_ENVIRONMENTS = {"dev", "test", "prod"}

## Return the current execution environment.
# Priority:
# 1. Environment explicitly provided by the Databricks Job.
# 2. Environment detected from the current compute tags.
def get_environment(
    spark,
    job_environment: Optional[str] = None,
) -> str:

    environment = (job_environment or "").strip().lower()

    # A Job parameter was provided: use it directly.
    if environment:
        return _validate_environment(environment)

    # No Job parameter was provided, typically during a manual run.
    environment = _get_environment_from_compute_tags(spark)

    return _validate_environment(environment)


## Retrieve the environment from the current compute tags.
def _get_environment_from_compute_tags(
    spark,
) -> str:

    raw_tags = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags","[]")

    parsed_tags = json.loads(raw_tags)

    tags = { str(item["key"]): str(item["value"])for item in parsed_tags }

    environment = tags.get(ENVIRONMENT_TAG)

    if not environment:
        raise ValueError(
            f"Required compute tag is missing: {ENVIRONMENT_TAG}"
        )

    return environment.strip().lower()


## Validate the resolved environment.
def _validate_environment(
    environment: str,
) -> str:

    if environment not in SUPPORTED_ENVIRONMENTS:
        raise ValueError(
            f"Unsupported environment '{environment}'. "
            f"Expected one of: {sorted(SUPPORTED_ENVIRONMENTS)}"
        )

    return environment
