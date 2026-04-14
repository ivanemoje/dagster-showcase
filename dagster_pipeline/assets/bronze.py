"""
dagster_pipeline/assets/bronze.py — Bronze layer asset.

Generates synthetic sales data and loads it into DuckDB.
Complexity: O(N) time, O(N) space — N=500, hard-bounded.
"""

import os
import random
from datetime import date, timedelta

import duckdb
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

DB_PATH = os.getenv("DB_PATH", "/data/pipeline.duckdb")


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def _init_bronze(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze_sales (
            sale_id     INTEGER  PRIMARY KEY,
            sale_date   DATE     NOT NULL,
            product     VARCHAR  NOT NULL,
            region      VARCHAR  NOT NULL,
            quantity    INTEGER  NOT NULL,
            unit_price  DECIMAL(10, 2) NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bronze_date    ON bronze_sales(sale_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bronze_product ON bronze_sales(product)")


@asset(
    group_name="medallion",
    description="Raw synthetic sales — Bronze layer.",
    compute_kind="python",
)
def extract_raw_sales(context: AssetExecutionContext) -> None:
    random.seed(42)
    products = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Tool Z"]
    regions  = ["EMEA", "APAC", "AMER"]
    today    = date.today()

    rows = [
        {
            "sale_id":    i,
            "sale_date":  today - timedelta(days=random.randint(0, 89)),
            "product":    random.choice(products),
            "region":     random.choice(regions),
            "quantity":   random.randint(1, 20),
            "unit_price": round(random.uniform(9.99, 299.99), 2),
        }
        for i in range(1, 501)
    ]
    df = pd.DataFrame(rows)

    with _conn() as conn:
        _init_bronze(conn)
        conn.execute("DELETE FROM bronze_sales")
        conn.execute("INSERT INTO bronze_sales SELECT * FROM df")
        count = conn.execute("SELECT COUNT(*) FROM bronze_sales").fetchone()[0]

    context.add_output_metadata({
        "rows_loaded": MetadataValue.int(count),
        "date_range":  MetadataValue.text(
            f"{df['sale_date'].min()} → {df['sale_date'].max()}"
        ),
    })
    context.log.info(f"Bronze: {count} rows loaded.")