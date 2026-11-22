# CDP-Datalake-GSOC-adb

## Project Overview

This Databricks bundle implements a staged SOC data lake:

- **Bronze**: incremental and initial ingestion from CDP/SSAE sources
- **Silver**: governed, processing-pattern-based transformation and write services
- **Governance**: Collibra metadata views and data treatment rules
- **Operations**: setup, control, and run-tracking tables

## Repository Structure

- `src/generic/`: shared configuration, API, control-table, and governance utilities
- `src/gsoc/bronze/`: Bronze readers, writers, and ingestion workflows
- `src/gsoc/restrictions/`: restriction detection and processing
- `src/gsoc/silver/`: Silver write operations and orchestration
- `notebooks/`: Databricks entry points for setup and ingestion jobs
- `resources/`: Databricks job definitions
- `devops/`: development and production deployment pipelines

## Getting Started

To deploy and manage this bundle, follow these steps:

### 1. Deployment

- Click the **deployment rocket** 🚀 in the left sidebar to open the **Deployments** panel, then click **Deploy**.

### 2. Running Jobs & Pipelines

- To run a deployed job or pipeline, hover over the resource in the **Deployments** panel and click the **Run** button.

### 3. Managing Resources

- Use the **Add** dropdown to add resources to the bundle.
- Click **Schedule** on a notebook within the bundle to create a **job definition** that schedules the notebook.

## Documentation

- For information on using **Declarative Automation Bundles in the workspace**, see: [Declarative Automation Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- For details on the **Declarative Automation Bundles format** used in this bundle, see: [Declarative Automation Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
