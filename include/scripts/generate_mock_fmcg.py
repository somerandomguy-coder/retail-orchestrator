from datetime import datetime, timedelta
from pathlib import Path
import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "include" / "data" / "raw"


def generate_pos_batch(
    output_dir: Path | str,
    batch_date: str,
    row_count: int = 3000,
    day_offset: int = 0
) -> str:
    """Generate high-integrity in-store POS transactions without data quality flaws."""
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = (target_dir / f"pos_transactions_{batch_date}.parquet").as_posix()

    id_offset = 1000000 + (day_offset * 10000)
    con = duckdb.connect()

    con.execute(f"""
        CREATE OR REPLACE TABLE pos_batch AS
        SELECT
            'POS-' || ({id_offset} + row_number() over())::VARCHAR AS transaction_id,
            'CUST-' || (1000 + (random() * 500)::INT)::VARCHAR AS customer_id,
            'BSK-' || (50000 + (random() * 2000)::INT)::VARCHAR AS basket_id,
            TIMESTAMP '{batch_date} 08:00:00' + INTERVAL 1 SECOND * (random() * 43200)::INT AS transaction_timestamp,
            (random() * 100 + 1)::INT AS product_id,
            ['Snacks', 'Dairy', 'Produce', 'Beverages', 'Bakery'][(floor(random() * 5) + 1)::INT] AS product_category,
            (floor(random() * 4) + 1)::INT AS quantity,
            ROUND((random() * 20 + 1.5)::NUMERIC, 2) AS unit_price,
            'POS' AS channel
        FROM generate_series(1, {row_count});
    """)

    con.execute(f"COPY pos_batch TO '{output_path}' (FORMAT PARQUET)")
    con.close()
    return output_path


def generate_marketplace_batch(
    output_dir: Path | str,
    batch_date: str,
    row_count: int = 1500,
    day_offset: int = 0
) -> str:
    """Generate 3rd-party marketplace transactions containing realistic planted quality bugs.

    Planted quality bugs:
    - Null customer IDs (~8% of records) simulating guest checkouts / sync errors.
    - Negative or zero unit prices (~6% of records) simulating refund sync flaws.
    - Duplicate transaction IDs simulating payment retry storms.
    """
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = (target_dir / f"marketplace_transactions_{batch_date}.parquet").as_posix()

    id_offset = 2000000 + (day_offset * 10000)
    con = duckdb.connect()

    con.execute(f"""
        CREATE OR REPLACE TABLE marketplace_batch AS
        SELECT
            -- Planted Bug 1: Duplicate transaction IDs on ~5% of records
            CASE
                WHEN (random() < 0.05) THEN 'MKT-DUP-99999'
                ELSE 'MKT-' || ({id_offset} + row_number() over())::VARCHAR
            END AS transaction_id,

            -- Planted Bug 2: Null customer_ids on ~8% of records
            CASE
                WHEN (random() < 0.08) THEN NULL
                ELSE 'CUST-' || (1000 + (random() * 500)::INT)::VARCHAR
            END AS customer_id,

            'BSK-' || (70000 + (random() * 2000)::INT)::VARCHAR AS basket_id,
            TIMESTAMP '{batch_date} 00:00:00' + INTERVAL 1 SECOND * (random() * 86400)::INT AS transaction_timestamp,
            (random() * 100 + 1)::INT AS product_id,
            ['Snacks', 'Dairy', 'Produce', 'Beverages', 'Bakery'][(floor(random() * 5) + 1)::INT] AS product_category,
            (floor(random() * 4) + 1)::INT AS quantity,

            -- Planted Bug 3: Negative unit prices on ~6% of records
            CASE
                WHEN (random() < 0.06) THEN ROUND((-1.0 * (random() * 15 + 1.0))::NUMERIC, 2)
                ELSE ROUND((random() * 30 + 2.0)::NUMERIC, 2)
            END AS unit_price,

            'MARKETPLACE' AS channel
        FROM generate_series(1, {row_count});
    """)

    con.execute(f"COPY marketplace_batch TO '{output_path}' (FORMAT PARQUET)")
    con.close()
    return output_path


def generate_all_mock_data(
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
    days: int = 3,
    base_date: datetime = None
) -> dict:
    """Generate both POS and Marketplace batches for the specified day span."""
    if base_date is None:
        base_date = datetime(2026, 10, 1)

    generated = {"pos": [], "marketplace": []}
    for day_offset in range(days):
        batch_date = (base_date + timedelta(days=day_offset)).strftime("%Y-%m-%d")
        pos_path = generate_pos_batch(output_dir, batch_date, row_count=3000, day_offset=day_offset)
        mkt_path = generate_marketplace_batch(output_dir, batch_date, row_count=1500, day_offset=day_offset)
        generated["pos"].append(pos_path)
        generated["marketplace"].append(mkt_path)
        print(f"Generated clean POS: {pos_path}")
        print(f"Generated dirty Marketplace: {mkt_path}")

    return generated


if __name__ == "__main__":
    generate_all_mock_data()
