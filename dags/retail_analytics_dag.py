from datetime import datetime
from pathlib import Path
import profile
from airflow.decorators import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping

# Base container paths
AIRFLOW_HOME = Path("/usr/local/airflow")
DBT_PROJECT_PATH = AIRFLOW_HOME / "include" / "dbt_retail"
DUCKDB_FILE_PATH = AIRFLOW_HOME / "include" / "data" / "retail.duckdb"

profile_config = ProfileConfig(
    profile_name = "retail_duckdb",
    target_name="dev",
    profile_mapping=DuckDBUserPasswordProfileMapping(
        conn_id="duckdb_default",
        profile_args={"path": str(DUCKDB_FILE_PATH)}
    )
)


@dag(
    dag_id="retail_analytics_pipeline",
    start_date=datetime(2026, 10, 1),
    catchup=False,
)
def retail_pipeline():

    @task
    def generate_daily_batch():

        return "Batch ready"

    dbt_models = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/bin/dbt"
        ),
    )


    generate_daily_batch() >> dbt_models


retail_pipeline()
