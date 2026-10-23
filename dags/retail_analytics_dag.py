from datetime import datetime, timedelta
import logging
from pathlib import Path

try:
    from airflow.sdk import dag, task, Asset
except ImportError:
    from airflow.decorators import dag, task
    from airflow.datasets import Dataset as Asset

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles import DuckDBUserPasswordProfileMapping

# Container execution base paths
AIRFLOW_HOME = Path("/usr/local/airflow")
DBT_PROJECT_PATH = AIRFLOW_HOME / "include" / "dbt_retail"
DUCKDB_FILE_PATH = AIRFLOW_HOME / "include" / "data" / "retail.duckdb"
RAW_DATA_PATH = AIRFLOW_HOME / "include" / "data" / "raw"
RETAIL_ASSET = Asset("duckdb://include/data/retail.duckdb")

logger = logging.getLogger("airflow.task")


def pipeline_failure_alert(context):
    """Simulated enterprise notification alert handler (e.g., Slack / PagerDuty webhook)."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    run_id = context.get("run_id", "unknown")
    log_url = getattr(task_instance, "log_url", "N/A")
    exception = context.get("exception", "Data quality assertion failed")

    logger.error(
        f"[PIPELINE ALERT] Data Quality Breach in DAG '{dag_id}' on Task '{task_id}' (Run: {run_id}).\n"
        f"Log URL: {log_url}\n"
        f"Exception: {exception}"
    )


profile_config = ProfileConfig(
    profile_name="retail_duckdb",
    target_name="dev",
    profile_mapping=DuckDBUserPasswordProfileMapping(
        conn_id="duckdb_default",
        profile_args={"path": str(DUCKDB_FILE_PATH)},
    ),
)

DAG_DOCS = """
# Retail Analytics Data Pipeline with Astronomer Cosmos

### Architecture Overview
This pipeline orchestrates multi-channel retail transaction ingestion and transformations:
1. **Raw Landing**: Validates landing partitions for **In-Store POS** and **Marketplace Partner** feeds.
2. **Staging & Quality Contracts**:
   - `stg_pos_transactions`: Passes all generic schema tests (unique, not_null, accepted_values).
   - `stg_marketplace_transactions`: Subject to rigorous data quality tests. Intentionally planted anomalies
     (null customer IDs, duplicate transaction IDs) fail tests at this staging barrier.
3. **Cosmos Task Isolation**: `RenderConfig(test_behavior=TestBehavior.AFTER_EACH)` runs tests immediately
   after staging models. The test failure isolates the dirty branch in the Airflow Graph View, protecting
   downstream marts (`int_customer_baskets`, `fct_customer_loyalty`, `dim_products`) from corruption.
"""


@dag(
    dag_id="retail_analytics_pipeline",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    schedule="@daily",
    tags=["retail", "dbt", "cosmos", "data-quality"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=10),
        "on_failure_callback": pipeline_failure_alert,
    },
    doc_md=DAG_DOCS,
)
def retail_pipeline():

    @task(outlets=[RETAIL_ASSET])
    def generate_daily_batch() -> dict:
        """Verify and ingest multi-source landing batches (POS & Marketplace).

        Checks raw landing directory, triggers mock generation if partitions are missing,
        and publishes an audit manifest to XCom.
        """
        raw_dir = RAW_DATA_PATH
        raw_dir.mkdir(parents=True, exist_ok=True)

        from include.scripts.generate_mock_fmcg import generate_all_mock_data
        pos_files = list(raw_dir.glob("pos_transactions_*.parquet"))
        mkt_files = list(raw_dir.glob("marketplace_transactions_*.parquet"))

        if not pos_files or not mkt_files:
            generate_all_mock_data(output_dir=raw_dir)
            pos_files = list(raw_dir.glob("pos_transactions_*.parquet"))
            mkt_files = list(raw_dir.glob("marketplace_transactions_*.parquet"))

        manifest = {
            "ingestion_timestamp": datetime.utcnow().isoformat(),
            "status": "Ready for dbt transformation",
            "pos_partitions_found": len(pos_files),
            "marketplace_partitions_found": len(mkt_files),
            "pos_files": [f.name for f in pos_files],
            "marketplace_files": [f.name for f in mkt_files],
        }
        logger.info(f"Ingestion batch verified: {manifest}")
        return manifest

    dbt_models = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/bin/dbt"
        ),
        render_config=RenderConfig(
            test_behavior=TestBehavior.AFTER_EACH
        ),
    )

    generate_daily_batch() >> dbt_models


retail_pipeline()
