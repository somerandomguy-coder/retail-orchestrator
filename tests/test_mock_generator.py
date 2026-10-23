from pathlib import Path
import duckdb
from include.scripts.generate_mock_fmcg import generate_pos_batch, generate_marketplace_batch


def test_pos_batch_is_clean(tmp_path: Path):
    file_path = generate_pos_batch(tmp_path, "2026-10-01", row_count=500)
    assert Path(file_path).exists()

    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{Path(file_path).as_posix()}')").df()
    con.close()

    assert len(df) == 500
    assert df["customer_id"].isnull().sum() == 0
    assert (df["quantity"] <= 0).sum() == 0
    assert (df["unit_price"] <= 0).sum() == 0
    assert df["transaction_id"].nunique() == len(df)
    assert set(df["channel"].unique()) == {"POS"}


def test_marketplace_batch_has_planted_bugs(tmp_path: Path):
    file_path = generate_marketplace_batch(tmp_path, "2026-10-01", row_count=500)
    assert Path(file_path).exists()

    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{Path(file_path).as_posix()}')").df()
    con.close()

    # Verify intentional data quality flaws
    assert df["customer_id"].isnull().sum() > 0, "Expected null customer_ids in marketplace feed"
    assert (df["unit_price"] <= 0).sum() > 0, "Expected negative/zero unit prices in marketplace feed"
    assert df["transaction_id"].nunique() < len(df), "Expected duplicate transaction_ids in marketplace feed"
    assert set(df["channel"].unique()) == {"MARKETPLACE"}
