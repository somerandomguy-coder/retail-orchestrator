from datetime import datetime, timedelta
from pathlib import Path
import duckdb

# Resolve directory relative to this script so it works from any working directory
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent if SCRIPT_DIR.name in ["scripts", "include"] else SCRIPT_DIR
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

base_date = datetime(2026, 10, 1)

for day_offset in range(3):
    batch_date = (base_date + timedelta(days=day_offset)).strftime("%Y-%m-%d")

    # Offset ID base by day so transaction_id remains globally unique
    id_offset = 1000000 + (day_offset * 10000)
    output_path = (OUTPUT_DIR / f"transactions_{batch_date}.parquet").as_posix()

    con.execute(f"""
        CREATE OR REPLACE TABLE raw_batch AS
        SELECT
            ({id_offset} + row_number() over())::VARCHAR AS transaction_id,
            (1000 + (random() * 500)::INT)::VARCHAR AS customer_id,
            (50000 + (random() * 2000)::INT)::VARCHAR AS basket_id,
            TIMESTAMP '{batch_date} 08:00:00' + INTERVAL 1 SECOND * (random() * 43200)::INT AS transaction_timestamp,
            (random() * 100)::INT AS product_id,
            ['Snacks', 'Dairy', 'Produce', 'Beverages', 'Bakery'][(floor(random() * 5) + 1)::INT] AS product_category,
            (floor(random() * 4) + 1)::INT AS quantity,
            ROUND((random() * 20 + 1.5)::NUMERIC, 2) AS unit_price
        FROM generate_series(1, 5000);
    """)

    con.execute(f"COPY raw_batch TO '{output_path}' (FORMAT PARQUET)")
    print(f"Generated {output_path}")

con.close()
