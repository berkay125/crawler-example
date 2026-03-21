#!/usr/bin/env bash
set -euo pipefail

# Automated concurrency check:
# 1) Starts crawl in background.
# 2) Runs search probes while crawl is still active.
# 3) Verifies indexed page count increases over time.

DB_PATH="${1:-concurrency_check.db}"
ORIGIN_URL="${2:-https://docs.python.org/3/}"
DEPTH="${3:-2}"
PYTHON_BIN=".venv/bin/python"
CLI_FILE="cli.py"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "ERROR: ${PYTHON_BIN} not found or not executable."
  echo "Create a virtual environment and install dependencies first."
  exit 1
fi

if [[ ! -f "${CLI_FILE}" ]]; then
  echo "ERROR: ${CLI_FILE} not found. Run this script from project root."
  exit 1
fi

rm -f "${DB_PATH}"

echo "[1/5] Starting background crawl"
"${PYTHON_BIN}" "${CLI_FILE}" --db "${DB_PATH}" crawl "${ORIGIN_URL}" "${DEPTH}" > "${DB_PATH}.crawl.log" 2>&1 &
CRAWL_PID=$!

echo "    crawl pid: ${CRAWL_PID}"

cleanup() {
  if kill -0 "${CRAWL_PID}" 2>/dev/null; then
    kill "${CRAWL_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

db_ready() {
  "${PYTHON_BIN}" - <<PY
import sqlite3
conn = sqlite3.connect("${DB_PATH}")
ready = conn.execute(
    "select count(*) from sqlite_master where type='table' and name='pages'"
).fetchone()[0]
print(1 if ready else 0)
conn.close()
PY
}

get_pages_count() {
  "${PYTHON_BIN}" - <<PY
import sqlite3
try:
    conn = sqlite3.connect("${DB_PATH}")
    print(conn.execute("select count(*) from pages").fetchone()[0])
    conn.close()
except Exception:
    print(0)
PY
}

echo "[2/5] Waiting for crawler to build initial index"
for _ in $(seq 1 60); do
  if ! kill -0 "${CRAWL_PID}" 2>/dev/null; then
    echo "ERROR: crawler finished too quickly; cannot prove concurrent search."
    echo "Try again with a larger depth or slower origin site."
    exit 1
  fi

  if [[ "$(db_ready)" -eq 1 ]]; then
    COUNT_NOW="$(get_pages_count)"
    if [[ "${COUNT_NOW}" -ge 25 ]]; then
      break
    fi
  fi
  sleep 1
done

echo "[3/5] Running first search probe during active crawl"
"${PYTHON_BIN}" "${CLI_FILE}" --db "${DB_PATH}" search "Python" --limit 10 > "${DB_PATH}.search1.log"
COUNT_1="$(get_pages_count)"
echo "    pages after search #1: ${COUNT_1}"

sleep 3

if ! kill -0 "${CRAWL_PID}" 2>/dev/null; then
  echo "ERROR: crawler stopped before second concurrent probe."
  echo "Try again with depth=3 to force longer indexing."
  exit 1
fi

echo "[4/5] Running second search probe during active crawl"
"${PYTHON_BIN}" "${CLI_FILE}" --db "${DB_PATH}" search "asyncio" --limit 10 > "${DB_PATH}.search2.log"
COUNT_2="$(get_pages_count)"
echo "    pages after search #2: ${COUNT_2}"

if [[ "${COUNT_2}" -lt "${COUNT_1}" ]]; then
  echo "ERROR: page count decreased unexpectedly (${COUNT_1} -> ${COUNT_2})."
  exit 1
fi

echo "[5/5] Waiting for crawl completion"
wait "${CRAWL_PID}"
FINAL_COUNT="$(get_pages_count)"
ERROR_COUNT="$("${PYTHON_BIN}" - <<PY
import sqlite3
conn = sqlite3.connect("${DB_PATH}")
print(conn.execute("select count(*) from pages where error is not null").fetchone()[0])
conn.close()
PY
)"

if [[ "${FINAL_COUNT}" -lt "${COUNT_2}" ]]; then
  echo "ERROR: final count is lower than in-flight count (${COUNT_2} -> ${FINAL_COUNT})."
  exit 1
fi

trap - EXIT

echo ""
echo "CONCURRENCY CHECK PASSED"
echo "- db: ${DB_PATH}"
echo "- pages during search #1: ${COUNT_1}"
echo "- pages during search #2: ${COUNT_2}"
echo "- final pages: ${FINAL_COUNT}"
echo "- error pages: ${ERROR_COUNT}"
echo "- logs: ${DB_PATH}.crawl.log, ${DB_PATH}.search1.log, ${DB_PATH}.search2.log"
