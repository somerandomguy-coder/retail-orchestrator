import ast
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DAGS_DIR = PROJECT_ROOT / "dags"


def test_dag_python_syntax():
    """Verify that all Python files in dags/ are syntactically valid with zero errors."""
    dag_files = list(DAGS_DIR.glob("*.py"))
    assert len(dag_files) > 0, "No DAG files found in dags/ directory"

    for dag_file in dag_files:
        content = dag_file.read_text(encoding="utf-8")
        # Compile source to verify no syntax errors
        compiled = compile(content, str(dag_file), "exec")
        assert compiled is not None
        # Parse AST tree to verify syntax integrity
        tree = ast.parse(content, filename=str(dag_file))
        assert isinstance(tree, ast.Module)


def test_retail_dag_structure():
    """Verify key DAG definitions and components exist in retail_analytics_dag.py."""
    retail_dag_file = DAGS_DIR / "retail_analytics_dag.py"
    assert retail_dag_file.exists()
    content = retail_dag_file.read_text(encoding="utf-8")

    assert "retail_analytics_pipeline" in content
    assert "generate_daily_batch" in content
    assert "dbt_transformations" in content
    assert "DbtTaskGroup" in content
    assert "TestBehavior.AFTER_EACH" in content
    assert "pipeline_failure_alert" in content
    assert "retail_duckdb" in content


@pytest.mark.skipif(
    not Path("/usr/local/airflow").exists(),
    reason="Full DagBag validation runs inside Astro Airflow container"
)
def test_dagbag_import_in_container():
    """Run full Airflow DagBag parsing when executing within Astro/Docker container."""
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

    dag = dagbag.get_dag("retail_analytics_pipeline")
    assert dag is not None
    assert set(dag.tags) >= {"retail", "dbt", "cosmos"}
    assert "generate_daily_batch" in [t.task_id for t in dag.tasks]
