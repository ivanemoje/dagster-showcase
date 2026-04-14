"""
api/app.py — FastAPI entry point for the Dagster Sales Pipeline.

Exposes:
  GET  /health                        - liveness probe
  GET  /contract                      - serve the data contract JSON
  POST /pipeline/run                  - trigger full pipeline (async via Celery)
  GET  /pipeline/status/{task_id}     - poll Celery task status
  GET  /data/daily-summary            - cached gold_daily_summary (Redis TTL 300s)
  GET  /data/top-products             - cached gold_top_products  (Redis TTL 300s)
  GET  /data/bronze                   - raw bronze row count + sample
"""

import json
import os
from pathlib import Path

import duckdb
import redis
from celery import Celery
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DB_PATH   = os.getenv("DB_PATH",   "/data/pipeline.duckdb")
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# ---------------------------------------------------------------------------
# Celery app (broker + backend both backed by Redis)
# ---------------------------------------------------------------------------

celery_app = Celery(
    "dagster_pipeline",
    broker=REDIS_URL,
    backend=REDIS_URL,
)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    # AI-readiness: track task results indefinitely so LLM agents can poll
    result_expires=3600,
)

# ---------------------------------------------------------------------------
# Redis client (for manual caching of query results)
# ---------------------------------------------------------------------------

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Dagster Sales Pipeline API",
    description="REST interface for the Bronze→Silver→Gold medallion pipeline.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


def _db() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def _cached_query(cache_key: str, sql: str) -> list[dict]:
    """
    Check Redis first; on miss execute SQL and cache the result.
    Space: O(result_set) in Redis — bounded by low-cardinality Gold tables.
    Time:  O(1) cache hit, O(N log N) cache miss.
    """
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    with _db() as conn:
        try:
            df = conn.execute(sql).df()
        except duckdb.CatalogException:
            return []

    rows = df.to_dict(orient="records")
    redis_client.setex(cache_key, CACHE_TTL, json.dumps(rows, default=str))
    return rows


# ---------------------------------------------------------------------------
# Celery task — materialise all Dagster assets inside the worker
# ---------------------------------------------------------------------------

@celery_app.task(bind=True, name="pipeline.run_full")
def run_full_pipeline(self):
    """
    Runs the complete medallion pipeline.  Executed by the Celery worker
    process so the API stays non-blocking.
    """
    from dagster import materialize

    # Import here to avoid import-time side effects in the API process
    from dagster_pipeline.assets.bronze  import extract_raw_sales
    from dagster_pipeline.assets.silver  import silver_sales
    from dagster_pipeline.assets.gold    import gold_daily_summary, gold_top_products

    result = materialize(
        [extract_raw_sales, silver_sales, gold_daily_summary, gold_top_products]
    )

    # Bust cache so next GET reflects fresh data
    for key in ["gold_daily_summary", "gold_top_products", "bronze_sample"]:
        redis_client.delete(key)

    return {"success": result.success}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["ops"])
def health():
    """Liveness + readiness probe — checks Redis connectivity."""
    try:
        redis_client.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {"status": "ok", "redis": redis_ok}


@app.get("/contract", tags=["ops"])
def get_contract():
    """Returns the machine-readable data contract (JSON Schema)."""
    contract_path = Path("/app/contracts/sales_pipeline_v1.json")
    if not contract_path.exists():
        raise HTTPException(status_code=404, detail="Contract file not found")
    return JSONResponse(content=json.loads(contract_path.read_text()))


@app.post("/pipeline/run", status_code=202, tags=["pipeline"])
def trigger_pipeline():
    """
    Enqueues a full pipeline run via Celery.
    Returns a task_id to poll for completion.
    """
    task = run_full_pipeline.delay()
    return {"task_id": task.id, "status": "queued"}


@app.get("/pipeline/status/{task_id}", tags=["pipeline"])
def pipeline_status(task_id: str):
    """Poll the status of an async pipeline run."""
    result = celery_app.AsyncResult(task_id)
    return {
        "task_id": task_id,
        "status":  result.status,
        "result":  result.result if result.ready() else None,
    }


@app.get("/data/daily-summary", tags=["data"])
def daily_summary():
    """Gold: daily revenue + 7-day rolling average. Cached 300s."""
    rows = _cached_query(
        "gold_daily_summary",
        "SELECT * FROM gold_daily_summary ORDER BY sale_date DESC LIMIT 30",
    )
    return {"rows": rows, "count": len(rows)}


@app.get("/data/top-products", tags=["data"])
def top_products():
    """Gold: product leaderboard ranked by total revenue. Cached 300s."""
    rows = _cached_query(
        "gold_top_products",
        "SELECT * FROM gold_top_products ORDER BY rank",
    )
    return {"rows": rows, "count": len(rows)}


@app.get("/data/bronze", tags=["data"])
def bronze_sample():
    """Bronze: row count + 5-row sample. Cached 300s."""
    rows = _cached_query(
        "bronze_sample",
        "SELECT * FROM bronze_sales ORDER BY sale_id LIMIT 5",
    )
    try:
        with _db() as conn:
            total = conn.execute("SELECT COUNT(*) FROM bronze_sales").fetchone()[0]
    except Exception:
        total = 0
    return {"total_rows": total, "sample": rows}