"""
dagster_pipeline/assets/silver.py — Silver layer asset.

Validates and enriches bronze_sales.
Complexity: O(N) scan + predicate filter before any projection.
"""

import os

import duckdb
from dagster import AssetExecutionContext, MetadataValue, asset

from dagster_pipeline.assets.bronze import extract_raw_sales

DB_PATH = os.getenv("DB_PATH", "/data/pipeline.duckdb")


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def _init_silver(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver_sales (
            sale_id     INTEGER  PRIMARY KEY,
            sale_date   DATE     NOT NULL,
            product     VARCHAR  NOT NULL,
            region      VARCHAR  NOT NULL,
            quantity    INTEGER  NOT NULL,
            unit_price  DECIMAL(10, 2) NOT NULL,
            revenue     DECIMAL(10, 2) NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_silver_date    ON silver_sales(sale_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_silver_product ON silver_sales(product)")


@asset(
    deps=[extract_raw_sales],
    group_name="medallion",
    description="Validated & enriched sales — Silver layer.",
    compute_kind="duckdb",
)
def silver_sales(context: AssetExecutionContext) -> None:
    with _conn() as conn:
        _init_silver(conn)
        conn.execute("DELETE FROM silver_sales")
        # Predicate pushed before projection — filters at source scan.
        conn.execute("""
            INSERT INTO silver_sales
            SELECT
                sale_id,
                sale_date,
                product,
                region,
                quantity,
                unit_price,
                ROUND(quantity * unit_price, 2) AS revenue
            FROM bronze_sales
            WHERE quantity   > 0
              AND unit_price > 0
        """)
        row, = conn.execute("""
            SELECT COUNT(*), ROUND(SUM(revenue), 2), ROUND(AVG(unit_price), 2)
            FROM silver_sales
        """).fetchone(),
        count, total_rev, avg_price = row

    context.add_output_metadata({
        "rows_enriched":  MetadataValue.int(count),
        "total_revenue":  MetadataValue.float(float(total_rev)),
        "avg_unit_price": MetadataValue.float(float(avg_price)),
    })
    context.log.info(f"Silver: {count} rows, ${total_rev} total revenue.")