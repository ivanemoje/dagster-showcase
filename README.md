# Dagster Sales Pipeline

A production-grade **Bronze → Silver → Gold** medallion pipeline built with Dagster, DuckDB, FastAPI, Celery, and Redis. Fully containerised, CI/CD-ready, with a machine-readable data contract and live REST API.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Services                             │
│                                                             │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐  ┌─────────┐  │
│  │  FastAPI │   │  Celery  │   │  Dagster │  │ Flower  │  │
│  │  :8000   │   │  Worker  │   │  :3000   │  │  :5555  │  │
│  └────┬─────┘   └────┬─────┘   └──────────┘  └─────────┘  │
│       │              │                                      │
│       └──────┬───────┘                                      │
│              ▼                                              │
│         ┌─────────┐        ┌──────────┐                    │
│         │  Redis  │        │  DuckDB  │                    │
│         │  :6379  │        │ /data/   │                    │
│         │ broker  │        │pipeline  │                    │
│         │ + cache │        │.duckdb   │                    │
│         └─────────┘        └──────────┘                    │
└─────────────────────────────────────────────────────────────┘

Pipeline layers:
  extract_raw_sales  (Bronze)  →  silver_sales  (Silver)
                                      ↓              ↓
                             gold_daily_summary   gold_top_products
                                   (Gold)             (Gold)
```

---

## Exposed Ports

| Port   | Service              | URL                          |
|--------|----------------------|------------------------------|
| `8000` | FastAPI REST API     | http://localhost:8000        |
| `8000` | Swagger UI (OpenAPI) | http://localhost:8000/docs   |
| `3000` | Dagster Webserver UI | http://localhost:3000        |
| `5555` | Flower (Celery UI)   | http://localhost:5555        |
| `6379` | Redis                | redis://localhost:6379       |

---

## Quick Start

### 1. Clone & configure

```bash
git clone https://github.com/your-org/dagster-pipeline.git
cd dagster-pipeline
cp .env.example .env
```

### 2. Start all services

```bash
docker compose up --build -d
```

### 3. Trigger the pipeline via curl

```bash
# Trigger a full pipeline run (async)
curl -s -X POST http://localhost:8000/pipeline/run | jq .
# → {"task_id": "abc-123", "status": "queued"}

# Poll status
curl -s http://localhost:8000/pipeline/status/abc-123 | jq .
# → {"task_id": "...", "status": "SUCCESS", "result": {"success": true}}
```

### 4. Run smoke tests

```bash
bash smoke_test.sh
```

---

## curl Reference

```bash
# ── Ops ──────────────────────────────────────────────────────────────────────

# Health check
curl -s http://localhost:8000/health | jq .

# View the data contract (JSON Schema)
curl -s http://localhost:8000/contract | jq .layers | head -60

# ── Pipeline ─────────────────────────────────────────────────────────────────

# Trigger full pipeline run (returns task_id immediately)
curl -s -X POST http://localhost:8000/pipeline/run | jq .

# Poll task status (replace TASK_ID)
curl -s http://localhost:8000/pipeline/status/TASK_ID | jq .

# ── Data ─────────────────────────────────────────────────────────────────────

# Bronze: raw row count + 5-row sample
curl -s http://localhost:8000/data/bronze | jq .

# Gold: last 30 days of daily revenue + 7-day rolling avg
curl -s http://localhost:8000/data/daily-summary | jq .rows[0]

# Gold: product leaderboard
curl -s http://localhost:8000/data/top-products | jq .rows

# ── Swagger UI ───────────────────────────────────────────────────────────────
open http://localhost:8000/docs
```

---

## Project Structure

```
dagster-pipeline/
├── api/
│   ├── __init__.py
│   └── app.py                  # FastAPI app, Celery config, Redis caching
├── dagster_pipeline/
│   ├── __init__.py             # Dagster Definitions root
│   └── assets/
│       ├── __init__.py
│       ├── bronze.py           # extract_raw_sales asset
│       ├── silver.py           # silver_sales asset
│       └── gold.py             # gold_daily_summary + gold_top_products
├── contracts/
│   └── sales_pipeline_v1.json  # Machine-readable data contract (JSON Schema)
├── tests/
│   └── test_pipeline.py        # 27 pytest tests
├── example_data/
│   └── sample_sales.csv        # Example input rows
├── .github/
│   └── workflows/
│       └── ci-cd.yml           # Lint → Test → Build → Deploy
├── Dockerfile                  # Multi-stage, non-root runtime
├── docker-compose.yml          # All 6 services
├── requirements.txt            # Runtime deps
├── requirements-test.txt       # Test + lint deps
├── setup.py                    # Editable install
├── pyproject.toml              # pytest + ruff + dagster config
├── smoke_test.sh               # curl-based smoke test script
└── .env.example                # Environment variable template
```

---

## Data Contract

The contract lives at `contracts/sales_pipeline_v1.json` and is also served live at `GET /contract`.

It documents:

- **Schema** — column names, types, nullability, constraints for all 4 tables
- **Quality rules** — what Silver rejects and why
- **SLA freshness** — expected staleness per layer (minutes)
- **Pipeline guarantees** — idempotency, complexity bounds, caching behaviour

View it in the browser: http://localhost:8000/contract

---

## Caching & Celery

- All Gold-layer API reads are cached in **Redis** with a 300-second TTL. Cache is automatically busted when a pipeline run completes.
- Pipeline runs are dispatched as **Celery tasks** (broker: Redis), keeping the API non-blocking. Use `/pipeline/status/{task_id}` to poll.
- **Celery Beat** is included for scheduled runs (configure in `api/app.py`).
- **Flower** at `:5555` gives you a live Celery task monitor.

---

## Running Tests Locally

```bash
pip install -r requirements-test.txt
pip install -e .

DB_PATH=/tmp/test.duckdb pytest --cov=dagster_pipeline --cov=api -v
```

---

## CI/CD

The GitHub Actions workflow (`.github/workflows/ci-cd.yml`) runs three jobs on push to `main`:

1. **Lint & Test** — `ruff` + `pytest` with a live Redis service container
2. **Build & Push** — multi-stage Docker build → GHCR with layer caching
3. **Deploy** — Tailscale VPN → SSH → `docker compose pull && up`

### Required Secrets

| Secret                  | Description                        |
|-------------------------|------------------------------------|
| `TAILSCALE_CLIENT_ID`   | Tailscale OAuth client ID          |
| `TAILSCALE_CLIENT_SECRET` | Tailscale OAuth secret           |
| `SERVER_IP`             | VPS Tailscale IP                   |
| `DEPLOYER_USER`         | SSH deploy user                    |
| `DEPLOYER_SSH`          | SSH private key                    |
| `FOLDER_PATH`           | Repo path on the VPS               |

---

## AI Readiness

- Celery task results persist for 1 hour — LLM agents can fire-and-poll
- `/contract` exposes the full JSON Schema for tool-calling agents to introspect table structures before querying
- All endpoints return structured JSON with consistent field names
- Redis result backend is queryable by task ID across sessions