#!/usr/bin/env bash
# smoke_test.sh — verify all API endpoints are live
# Usage: bash smoke_test.sh [BASE_URL]
# Defaults to http://localhost:8000

set -euo pipefail

BASE="${1:-http://localhost:8000}"
PASS=0
FAIL=0

check() {
  local label="$1"
  local url="$2"
  local expected_field="$3"

  printf "%-40s" "$label"
  response=$(curl -sf "$url" 2>/dev/null) || { echo "❌  CURL FAILED"; ((FAIL++)); return; }

  if echo "$response" | grep -q "$expected_field"; then
    echo "✅  OK"
    ((PASS++))
  else
    echo "❌  MISSING field '$expected_field'"
    echo "    Response: $(echo "$response" | head -c 200)"
    ((FAIL++))
  fi
}

echo ""
echo "🔍  Dagster Pipeline Smoke Test"
echo "    Target: $BASE"
echo "────────────────────────────────────────────────"

check "GET /health"              "$BASE/health"              '"status"'
check "GET /contract"            "$BASE/contract"            '"layers"'
check "GET /data/bronze"         "$BASE/data/bronze"         '"total_rows"'
check "GET /data/daily-summary"  "$BASE/data/daily-summary"  '"rows"'
check "GET /data/top-products"   "$BASE/data/top-products"   '"rows"'

# Trigger a pipeline run and check we get a task_id back
echo ""
printf "%-40s" "POST /pipeline/run"
RUN_RESP=$(curl -sf -X POST "$BASE/pipeline/run" -H "Content-Type: application/json" 2>/dev/null) || \
  { echo "❌  CURL FAILED"; ((FAIL++)); }

if echo "$RUN_RESP" | grep -q '"task_id"'; then
  TASK_ID=$(echo "$RUN_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['task_id'])")
  echo "✅  OK  (task_id: $TASK_ID)"
  ((PASS++))

  # Poll status
  sleep 2
  printf "%-40s" "GET /pipeline/status/{task_id}"
  STATUS_RESP=$(curl -sf "$BASE/pipeline/status/$TASK_ID" 2>/dev/null) || \
    { echo "❌  CURL FAILED"; ((FAIL++)); }
  if echo "$STATUS_RESP" | grep -q '"status"'; then
    STATUS=$(echo "$STATUS_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
    echo "✅  OK  (status: $STATUS)"
    ((PASS++))
  fi
else
  echo "❌  MISSING task_id"
  ((FAIL++))
fi

echo "────────────────────────────────────────────────"
echo "Results: $PASS passed, $FAIL failed"
echo ""

[ "$FAIL" -eq 0 ] && exit 0 || exit 1