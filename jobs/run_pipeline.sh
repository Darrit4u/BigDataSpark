#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE="docker compose"
POSTGRES_USER="user"
POSTGRES_DB="lab2"
CLICKHOUSE_USER="user"
CLICKHOUSE_PASSWORD="password"
CLICKHOUSE_DB="lab2"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

wait_for_postgres() {
  log "Waiting for postgres..."
  for i in $(seq 1 60); do
    if docker exec postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
      log "Postgres is ready"
      return 0
    fi
    sleep 2
  done
  log "Postgres is not ready"
  exit 1
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
  log "ClickHouse is not ready"
  exit 1
}

download_jars(){
  log "Downloading JDBC jars inside spark container"
  docker exec -i spark bash -lc "cd /home/jovyan/work && bash ./download_jdbc.sh /home/jovyan/work/jdbc"
}

run_spark_jobs(){
  log "Running job: postgres_to_star.py"
  docker exec -i spark bash -lc "cd /home/jovyan/work && python postgres_to_star.py"

  log "Running job: star_to_clickhouse.py"
  docker exec -i spark bash -lc "cd /home/jovyan/work && python star_to_clickhouse.py"

}

run_postgres_checks() {
  log "PostgreSQL checks"
  docker exec -i postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) AS mock_data_rows FROM mock_data;"
  docker exec -i postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 'dim_customer' AS table_name, COUNT(*) AS rows_count FROM dim_customer UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;"
}

run_clickhouse_checks() {
  log "ClickHouse checks"
  docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -d "$CLICKHOUSE_DB" -q "SHOW TABLES;"
  docker exec -i clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -d "$CLICKHOUSE_DB" -q "SELECT 'mart_sales_by_products' AS table_name, count() AS rows_count FROM mart_sales_by_products UNION ALL SELECT 'mart_sales_by_customers', count() FROM mart_sales_by_customers UNION ALL SELECT 'mart_sales_by_time', count() FROM mart_sales_by_time UNION ALL SELECT 'mart_sales_by_stores', count() FROM mart_sales_by_stores UNION ALL SELECT 'mart_sales_by_suppliers', count() FROM mart_sales_by_suppliers UNION ALL SELECT 'mart_product_quality', count() FROM mart_product_quality;"
}

check_pg_star(){
  docker exec -i postgres psql -U user -d lab2 -c "SELECT COUNT(*) AS mock_data_rows FROM mock_data;"
}

check_null_in_star(){
  docker exec -i postgres psql -U user -d lab2 -c "SELECT source_customer_id, COUNT(*) FROM dim_customer WHERE source_customer_id IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1;"
  docker exec -i postgres psql -U user -d lab2 -c "SELECT source_seller_id, COUNT(*) FROM dim_seller WHERE source_seller_id IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1;"
  docker exec -i postgres psql -U user -d lab2 -c "SELECT source_product_id, COUNT(*) FROM dim_product WHERE source_product_id IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1;"
  docker exec -i postgres psql -U user -d lab2 -c "SELECT COUNT(*) FROM fact_sales WHERE customer_key IS NULL OR seller_key IS NULL OR product_key IS NULL OR store_key IS NULL OR supplier_key IS NULL OR date_key IS NULL;"
}

check_mart_clickhouse(){
  docker exec -i clickhouse clickhouse-client --user user --password password -d lab2 -q "SHOW TABLES;"
  docker exec -i clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_sales_by_customers;"
  docker exec -i clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_sales_by_products;"
  docker exec -i clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_sales_by_time;"
  docker exec -i clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_sales_by_stores;"
  docker exec -i clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_sales_by_suppliers;"
  docker exec -i clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_product_quality;"
}

report_clickhouse(){
  docker exec -i clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT * FROM mart_sales_by_products ORDER BY sales_rank, product_key LIMIT 10;"
  docker exec -i clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT * FROM mart_sales_by_customers ORDER BY spend_rank, customer_key LIMIT 10;"
  docker exec -i clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT year_num, month_num, total_revenue FROM mart_sales_by_time ORDER BY year_num, month_num;"
}


log "Starting docker services"
$COMPOSE up -d

wait_for_postgres
wait_for_clickhouse

check_pg_star
check_null_in_star

download_jars
run_spark_jobs

run_postgres_checks
run_clickhouse_checks

check_mart_clickhouse
report_clickhouse

log "Pipeline finished successfully"
