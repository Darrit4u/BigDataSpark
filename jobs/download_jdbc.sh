#!/usr/bin/env bash

TARGET_DIR="${1:-/home/jovyan/work/jdbc}"
mkdir -p "$TARGET_DIR"

POSTGRES_JDBC_VERSION="42.7.3"
CLICKHOUSE_JDBC_VERSION="0.6.3"
CASSANDRA_JDBC_WRAPPER_VERSION="4.13.0"

BASE_URL="https://repo1.maven.org/maven2"

download() {
  local url="$1"
  local out="$2"
  echo "Downloading $(basename "$out")"
  curl -fL "$url" -o "$out"
}

download \
  "$BASE_URL/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
  "$TARGET_DIR/postgresql-${POSTGRES_JDBC_VERSION}.jar"

download \
  "$BASE_URL/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}.jar" \
  "$TARGET_DIR/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}.jar"

download \
  "$BASE_URL/com/ing/data/cassandra-jdbc-wrapper/${CASSANDRA_JDBC_WRAPPER_VERSION}/cassandra-jdbc-wrapper-${CASSANDRA_JDBC_WRAPPER_VERSION}.jar" \
  "$TARGET_DIR/cassandra-jdbc-wrapper-${CASSANDRA_JDBC_WRAPPER_VERSION}.jar"

echo "Done. JDBC/client jars are in: $TARGET_DIR"
ls -lh "$TARGET_DIR"
