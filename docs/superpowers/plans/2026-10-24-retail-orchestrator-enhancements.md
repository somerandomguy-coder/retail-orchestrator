# Retail Orchestrator Enhancements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform the retail orchestrator project into an enterprise-grade pipeline showcasing multi-source retail ingestion, dirty data quality test isolation with Astronomer Cosmos, automated DAG integrity testing, Airflow UI screenshots, and a complete portfolio README.

**Architecture:** A modern data pipeline orchestrating in-store POS and unvetted marketplace partner Parquet data into DuckDB via dbt-core and Astronomer Cosmos on Apache Airflow. Cosmos executes granular dbt model runs and tests (`AFTER_EACH`), visually catching data anomalies at the staging boundary and halting downstream customer marts from corruption.

**Tech Stack:** Astro Runtime / Apache Airflow 2.10+, Astronomer Cosmos >=1.8.0, dbt-core, dbt-duckdb >=1.8.0, DuckDB >=1.0.0, Python 3.11+, pytest.

## Global Constraints
- Target workspace: `c:\Users\Nam\Projects\PythonProjects\retail-orchestrator`
- Airflow container base paths: `/usr/local/airflow/include/...`
- Every checkpoint must execute `git add`, `git commit`, and `git push origin main`.
- All paths must use forward slashes in python code for cross-platform container compatibility.

---

### Task 1: Multi-Source Mock Data Generator & Planted Quality Bugs

**Files:**
- Modify: `include/scripts/generate_mock_fmcg.py`
- Test: `tests/test_mock_generator.py`

**Interfaces:**
- Consumes: None (generates synthetic raw data using DuckDB)
- Produces: `generate_pos_batch(output_dir, batch_date, row_count)` and `generate_marketplace_batch(output_dir, batch_date, row_count)` writing Parquet files to `include/data/raw/`.

- [ ] **Step 1: Write test for generator functions**

Write `tests/test_mock_generator.py`:
```python
from pathlib import Path
import duckdb
from include.scripts.generate_mock_fmcg import generate_pos_batch, generate_marketplace_batch

def test_pos_batch_is_clean(tmp_path: Path):
    file_path = generate_pos_batch(tmp_path, "2026-10-01", row_count=500)
    assert Path(file_path).exists()
    
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
    con.close()
    
    assert len(df) == 500
    assert df["customer_id"].isnull().sum() == 0
    assert (df["quantity"] <= 0).sum() == 0
    assert (df["unit_price"] <= 0).sum() == 0
    assert df["transaction_id"].nunique() == len(df)

def test_marketplace_batch_has_planted_bugs(tmp_path: Path):
    file_path = generate_marketplace_batch(tmp_path, "2026-10-01", row_count=500)
    assert Path(file_path).exists()
    
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
    con.close()
    
    # Assert intentional bugs are present in marketplace feed
    assert df["customer_id"].isnull().sum() > 0, "Expected null customer_ids in marketplace feed"
    assert (df["unit_price"] <= 0).sum() > 0, "Expected negative/zero prices in marketplace feed"
    assert df["transaction_id"].nunique() < len(df), "Expected duplicate transaction_ids in marketplace feed"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_mock_generator.py -v`
Expected: FAIL with ImportError (functions not yet defined).

- [ ] **Step 3: Implement modular generator in `include/scripts/generate_mock_fmcg.py`**

Refactor `include/scripts/generate_mock_fmcg.py` with `generate_pos_batch` and `generate_marketplace_batch`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_mock_generator.py -v`
Expected: PASS.

- [ ] **Step 5: Run generator to populate `include/data/raw/`**

Run script to generate `pos_transactions_2026-10-01.parquet` through `2026-10-03` and `marketplace_transactions_2026-10-01.parquet` through `2026-10-03`.

- [ ] **Step 6: Commit and push**

```bash
git add include/scripts/generate_mock_fmcg.py tests/test_mock_generator.py
git commit -m "feat: implement multi-source retail mock generator with planted data quality flaws"
git push origin main
```

---

### Task 2: dbt Layer Expansion & Data Quality Test Gates

**Files:**
- Modify: `include/dbt_retail/models/staging/sources.yml`
- Create: `include/dbt_retail/models/staging/stg_pos_transactions.sql`
- Create: `include/dbt_retail/models/staging/stg_marketplace_transactions.sql`
- Modify: `include/dbt_retail/models/staging/schema.yml`
- Modify: `include/dbt_retail/models/intermediate/int_customer_baskets.sql`
- Modify: `include/dbt_retail/models/marts/fct_customer_loyalty.sql`
- Create: `include/dbt_retail/models/marts/dim_products.sql`
- Create: `include/dbt_retail/tests/assert_positive_basket_spend.sql`
- Modify: `include/dbt_retail/dbt_project.yml`

**Interfaces:**
- Consumes: Parquet files in `/usr/local/airflow/include/data/raw/`
- Produces: `stg_pos_transactions`, `stg_marketplace_transactions`, `int_customer_baskets`, `fct_customer_loyalty`, `dim_products`.

- [ ] **Step 1: Configure `sources.yml` for multi-source landing**

Update `sources.yml` with `pos_transactions` and `marketplace_transactions`.

- [ ] **Step 2: Create staging models**

Create `stg_pos_transactions.sql` and `stg_marketplace_transactions.sql` with channel tagging and consistent schema casting.

- [ ] **Step 3: Define schema tests in `schema.yml` and singular test**

Define `not_null` on `customer_id`, `unique` on `transaction_id`, accepted values on `product_category`.
Create `tests/assert_positive_basket_spend.sql`.

- [ ] **Step 4: Update intermediate and marts models**

Update `int_customer_baskets.sql` to aggregate valid baskets.
Update `fct_customer_loyalty.sql` to compute tier thresholds.
Create `dim_products.sql` to provide category pricing benchmarks.

- [ ] **Step 5: Verify dbt compilation and tests locally**

Run dbt or test script to confirm `stg_pos_transactions` passes tests and `stg_marketplace_transactions` fails schema tests as intended.

- [ ] **Step 6: Commit and push**

```bash
git add include/dbt_retail/
git commit -m "feat: expand dbt models with multi-source staging, dimensional marts, and quality test gates"
git push origin main
```

---

### Task 3: Airflow DAG Modernization & Astronomer Cosmos Orchestration

**Files:**
- Modify: `dags/retail_analytics_dag.py`
- Modify: `airflow_settings.yaml`
- Modify: `.env`

**Interfaces:**
- Consumes: `generate_pos_batch`, `generate_marketplace_batch`, dbt project at `include/dbt_retail`
- Produces: Airflow DAG `retail_analytics_pipeline` with Cosmos task group.

- [ ] **Step 1: Modernize `dags/retail_analytics_dag.py`**

- Remove unused `import profile`.
- Implement `@task def generate_daily_batch()` that verifies/generates batches and pushes manifest metadata to XCom.
- Add `pipeline_failure_alert` as `on_failure_callback`.
- Add rich markdown `dag.doc_md` rendered in the Airflow UI DAG Docs tab.
- Configure Cosmos `RenderConfig(test_behavior=TestBehavior.AFTER_EACH)` to isolate test nodes in the graph view.

- [ ] **Step 2: Configure `airflow_settings.yaml` and `.env`**

Add `duckdb_default` connection and variables in `airflow_settings.yaml`.
Configure `.env` with recommended Airflow settings.

- [ ] **Step 3: Commit and push**

```bash
git add dags/retail_analytics_dag.py airflow_settings.yaml .env
git commit -m "feat: modernize Airflow DAG with TaskFlow XCom manifest, Cosmos test isolation, and rich UI docs"
git push origin main
```

---

### Task 4: Automated CI DAG Integrity Testing

**Files:**
- Create: `tests/dags/test_dag_integrity.py`

**Interfaces:**
- Consumes: `dags/retail_analytics_dag.py`
- Produces: Pytest test suite for DAG validity.

- [ ] **Step 1: Write `tests/dags/test_dag_integrity.py`**

Test that Airflow parses all DAGs in `dags/` with zero import errors, verifies task IDs, tags, and acyclic properties.

- [ ] **Step 2: Run pytest**

Run: `pytest tests/ -v`
Expected: All tests pass.

- [ ] **Step 3: Commit and push**

```bash
git add tests/dags/test_dag_integrity.py
git commit -m "test: add automated DAG integrity and cycle detection test suite"
git push origin main
```

---

### Task 5: Astro Container Execution & Browser Subagent Graph View Capture

**Files:**
- Create: `docs/images/`
- Output: `docs/images/airflow_graph_view.png`

**Interfaces:**
- Consumes: Running Astro environment at `http://localhost:8080`
- Produces: Live UI screenshots of the Airflow Graph View showing the caught bug.

- [ ] **Step 1: Start Astro dev environment**

Run: `astro dev start` or verify container status.

- [ ] **Step 2: Trigger DAG run**

Trigger `retail_analytics_pipeline` and let it execute through ingestion, POS staging, and Marketplace staging.

- [ ] **Step 3: Dispatch browser subagent to capture UI**

Navigate to `http://localhost:8080`, view the Graph View, take screenshot showing green POS tasks and red Marketplace test failure.
Save screenshot to `docs/images/airflow_graph_view.png`.

- [ ] **Step 4: Commit and push**

```bash
git add docs/images/
git commit -m "docs: capture Airflow UI graph view screenshot showing Cosmos test isolation in action"
git push origin main
```

---

### Task 6: Enterprise Portfolio README Transformation

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: Architectural details, data lineage, screenshots from Task 5.
- Produces: Professional, comprehensive `README.md`.

- [ ] **Step 1: Write executive README.md**

Include project badges, architecture overview, Mermaid lineage diagrams, problem/solution deep dive, embedded screenshots with annotations, step-by-step local replication guide, and technical stack summary.

- [ ] **Step 2: Review links and formatting**

Verify all markdown links and images render correctly.

- [ ] **Step 3: Commit and push**

```bash
git add README.md
git commit -m "docs: publish enterprise portfolio README with architecture diagrams, screenshots, and replication guide"
git push origin main
```
