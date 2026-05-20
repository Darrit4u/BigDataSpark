#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOBS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$JOBS_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

COMPOSE="docker compose"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

fail() {
  echo "[FAIL] $*" >&2
  exit 1
}

ensure_env_file() {
  if [[ ! -f .env ]]; then
    log "File .env not found in project root, creating it from .env.example"
    cp .env.example .env
  fi
  sed -i 's/\r$//' .env .env.example
}

load_env() {
  set -a
  . ./.env
  set +a

  PGUSER="${PGUSER:-user}"
  PGDATABASE="${PGDATABASE:-lab2}"

  CLICKHOUSE_USER="${CLICKHOUSE_USER:-user}"
  CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-password}"
  CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-lab2}"

  CASSANDRA_KEYSPACE="${CASSANDRA_KEYSPACE:-lab2}"
  CASSANDRA_PYTHON_DRIVER_VERSION="${CASSANDRA_PYTHON_DRIVER_VERSION:-3.29.2}"
}

wait_for_postgres() {
  log "Waiting for postgres..."
  for i in $(seq 1 60); do
    if docker exec postgres pg_isready -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; then
      log "Postgres is ready"
      return 0
    fi
    sleep 2
  done
  fail "Postgres is not ready"
}

wait_for_clickhouse() {
  log "Waiting for clickhouse..."
  for i in $(seq 1 60); do
    if docker exec clickhouse bash -lc "wget -qO- http://localhost:8123/ping | grep -q 'Ok.'"; then
      log "ClickHouse is ready"
      return 0
    fi
    sleep 2
  done
  fail "ClickHouse is not ready"
}

wait_for_cassandra() {
  log "Waiting for cassandra..."
  for i in $(seq 1 90); do
    if docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; then
      log "Cassandra is ready"
      return 0
    fi
    sleep 2
  done
  fail "Cassandra is not ready"
}

run_checked() {
  local title="$1"
  shift
  log "$title"
  if ! "$@"; then
    fail "Command failed: $*"
  fi
}

extract_single_number() {
  local text="$1"
  local value
  value="$(echo "$text" | tr -d '\r' | awk '/^[[:space:]]*[0-9]+[[:space:]]*$/{gsub(/[[:space:]]/, "", $0); last=$0} END{print last}')"
  [[ -n "$value" ]] || return 1
  echo "$value"
}

download_jars() {
  run_checked "Downloading JDBC jars inside spark container" \
    docker exec -i spark bash -lc "cd /home/jovyan/work && bash ./scripts/download_jdbc.sh /home/jovyan/work/jdbc"
}

prepare_cassandra_schema() {
  log "Cassandra schema will be prepared inside star_to_cassandra.py"
}

ensure_cassandra_python_driver() {
  run_checked "Ensuring cassandra-driver in spark container" \
    docker exec -i spark bash -lc "python - <<'PY'
try:
    import cassandra  # noqa: F401
    print('cassandra-driver already installed')
except Exception:
    import subprocess
    subprocess.check_call(['python', '-m', 'pip', 'install', '--no-cache-dir', f'cassandra-driver==${CASSANDRA_PYTHON_DRIVER_VERSION}'])
    print('cassandra-driver installed')
PY"
}


run_spark_jobs() {
  run_checked "Running job: postgres_to_star.py" \
    docker exec -it spark bash -lc "cd /home/jovyan/work && python postgres_to_star.py"

  run_checked "Running job: star_to_clickhouse.py" \
    docker exec -i spark bash -lc "cd /home/jovyan/work && python star_to_clickhouse.py"

  run_checked "Running job: star_to_cassandra.py" \
    docker exec -i spark bash -lc "cd /home/jovyan/work && python star_to_cassandra.py"
}

run_postgres_checks() {
  log "PostgreSQL checks"

  local mock_rows
  mock_rows="$(docker exec -i postgres psql -U "$PGUSER" -d "$PGDATABASE" -At -c "SELECT COUNT(*) FROM mock_data;" | tr -d '\r' | tail -n 1)"
  log "mock_data rows: $mock_rows"

  local fact_rows
  fact_rows="$(docker exec -i postgres psql -U "$PGUSER" -d "$PGDATABASE" -At -c "SELECT COUNT(*) FROM fact_sales;" | tr -d '\r' | tail -n 1)"
  log "fact_sales rows: $fact_rows"
}

run_clickhouse_checks() {
  log "ClickHouse checks"

  run_checked "ClickHouse tables list" \
    docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -d "$CLICKHOUSE_DATABASE" -q "SHOW TABLES;"

  for table in mart_sales_by_products mart_sales_by_customers mart_sales_by_time mart_sales_by_stores mart_sales_by_suppliers mart_product_quality; do
    local rows
    rows="$(docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -d "$CLICKHOUSE_DATABASE" -q "SELECT count() FROM ${table};" | tr -d '\r' | tail -n 1)"
    log "ClickHouse ${table}: $rows"
  done
}

run_cassandra_checks() {
  log "Cassandra checks"

  local keyspaces_out
  keyspaces_out="$(docker exec -i cassandra cqlsh -e "DESCRIBE KEYSPACES;" 2>&1)" || fail "Cannot query Cassandra keyspaces"
  if ! echo "$keyspaces_out" | grep -Eq "(^|[[:space:]])${CASSANDRA_KEYSPACE}($|[[:space:]])"; then
    echo "$keyspaces_out"
    fail "Cassandra keyspace ${CASSANDRA_KEYSPACE} not found"
  fi

  for table in mart_sales_by_products mart_sales_by_customers mart_sales_by_time mart_sales_by_stores mart_sales_by_suppliers mart_product_quality; do
    local out
    out="$(docker exec -i cassandra cqlsh -e "SELECT COUNT(*) FROM ${CASSANDRA_KEYSPACE}.${table};" 2>&1)" || fail "Cassandra query failed for ${table}"
    if echo "$out" | grep -Eiq "InvalidRequest|SyntaxException|ConfigurationException|NoHostAvailable|Traceback|^error"; then
      echo "$out"
      fail "Cassandra query returned an error for ${table}"
    fi
    local rows
    rows="$(extract_single_number "$out")" || fail "Cannot parse Cassandra row count for ${table}"
    log "Cassandra ${table}: $rows"
  done
}

report_clickhouse() {
  log "ClickHouse report preview"
  run_checked "Top products report preview" \
    docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -d "$CLICKHOUSE_DATABASE" -q "SELECT * FROM mart_sales_by_products ORDER BY sales_rank, product_key LIMIT 10;"
  run_checked "Top customers report preview" \
    docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -d "$CLICKHOUSE_DATABASE" -q "SELECT * FROM mart_sales_by_customers ORDER BY spend_rank, customer_key LIMIT 10;"
  run_checked "Sales by time report preview" \
    docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -d "$CLICKHOUSE_DATABASE" -q "SELECT year_num, month_num, total_revenue FROM mart_sales_by_time ORDER BY year_num, month_num;"
}

main() {
  ensure_env_file
  load_env

  log "Starting docker services"
  run_checked "docker compose up -d" $COMPOSE up -d

  wait_for_postgres
  wait_for_clickhouse
  wait_for_cassandra

  download_jars
  ensure_cassandra_python_driver
  prepare_cassandra_schema
  run_spark_jobs

  run_postgres_checks
  run_clickhouse_checks
  run_cassandra_checks
  report_clickhouse

  log "Pipeline finished successfully"
}

main "$@"
