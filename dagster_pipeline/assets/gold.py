"""
dagster_pipeline/assets/gold.py — Gold layer assets.

gold_daily_summary  : O(N log N) GROUP BY + O(N) window fn (no self-join)
gold_top_products   : O(N log N) GROUP BY + ORDER BY
"""

import os

import duckdb
from dagster import AssetExecutionContext, MetadataValue, asset

from dagster_pipeline.assets.silver import silver_sales

DB_PATH = os.getenv("DB_PATH", "/data/pipeline.duckdb")


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def _init_gold(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold_daily_summary (
            sale_date        DATE           NOT NULL PRIMARY KEY,
            daily_revenue    DECIMAL(10, 2) NOT NULL,
            rolling_7d_rev   DECIMAL(10, 2) NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold_top_products (
            product       VARCHAR        NOT NULL PRIMARY KEY,
            total_revenue DECIMAL(10, 2) NOT NULL,
            total_units   INTEGER        NOT NULL,
            rank          INTEGER        NOT NULL
        )
    """)


@asset(
    deps=[silver_sales],
    group_name="medallion",
    description="Daily revenue + 7-day rolling avg — Gold layer.",
    compute_kind="duckdb",
)
def gold_daily_summary(context: AssetExecutionContext) -> None:
    with _conn() as conn:
        _init_gold(conn)
        conn.execute("DELETE FROM gold_daily_summary")
        conn.execute("""
            INSERT INTO gold_daily_summary
            WITH daily AS (
                SELECT sale_date, ROUND(SUM(revenue), 2) AS daily_revenue
                FROM silver_sales
                GROUP BY sale_date
            )
            SELECT
                sale_date,
                daily_revenue,
                ROUND(
                    AVG(daily_revenue) OVER (
                        ORDER BY sale_date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ), 2
                ) AS rolling_7d_rev
            FROM daily
            ORDER BY sale_date
        """)
        count = conn.execute("SELECT COUNT(*) FROM gold_daily_summary").fetchone()[0]
        sample = conn.execute("""
            SELECT sale_date, daily_revenue, rolling_7d_rev
            FROM gold_daily_summary ORDER BY sale_date DESC LIMIT 3
        """).df()

    context.add_output_metadata({
        "days_covered":  MetadataValue.int(count),
        "recent_sample": MetadataValue.md(sample.to_markdown(index=False)),
    })
    context.log.info(f"Gold daily: {count} date rows materialised.")


@asset(
    deps=[silver_sales],
    group_name="medallion",
    description="Product leaderboard by revenue — Gold layer.",
    compute_kind="duckdb",
)
def gold_top_products(context: AssetExecutionContext) -> None:
    with _conn() as conn:
        _init_gold(conn)
        conn.execute("DELETE FROM gold_top_products")
        conn.execute("""
            INSERT INTO gold_top_products
            SELECT
                product,
                ROUND(SUM(revenue), 2)   AS total_revenue,
                SUM(quantity)            AS total_units,
                DENSE_RANK() OVER (ORDER BY SUM(revenue) DESC) AS rank
            FROM silver_sales
            GROUP BY product
            ORDER BY rank
        """)
        leaderboard = conn.execute(
            "SELECT rank, product, total_revenue, total_units FROM gold_top_products ORDER BY rank"
        ).df()

    context.add_output_metadata({
        "product_count": MetadataValue.int(len(leaderboard)),
        "leaderboard":   MetadataValue.md(leaderboard.to_markdown(index=False)),
    })
    context.log.info(f"Gold products:\n{leaderboard.to_string(index=False)}")