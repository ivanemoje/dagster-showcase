# ── Stage 1: dependency builder ─────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Stage 2: runtime image ───────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

LABEL org.opencontainers.image.title="Dagster Sales Pipeline"
LABEL org.opencontainers.image.description="Bronze→Silver→Gold medallion pipeline with FastAPI + Celery"
LABEL org.opencontainers.image.source="https://github.com/your-org/dagster-pipeline"

# Non-root user for security
RUN addgroup --system appgroup \
 && adduser  --system --ingroup appgroup appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy source
COPY dagster_pipeline/ ./dagster_pipeline/
COPY api/           ./api/
COPY contracts/     ./contracts/
COPY pyproject.toml    .

# Persistent DuckDB data directory
RUN mkdir -p /data && chown appuser:appgroup /data

USER appuser

# Ports
# 3000 — Dagster webserver UI
# 8000 — FastAPI REST API
# 5555 — Flower (Celery monitor)
EXPOSE 3000 8000 5555

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DB_PATH=/data/pipeline.duckdb

# Default: run the FastAPI server
CMD [uvicorn, api.app:app, --host, 0.0.0.0, --port, 8000]