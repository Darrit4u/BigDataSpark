from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, functions as F

from common.jdbc import JdbcConnection
from common.spark_job import SparkJob, env
from source_mart_specs import source_query


@dataclass(frozen=True)
class CassandraMartSpec:
    name: str
    target_table: str
    source_query: str


def schema_statements(keyspace: str) -> list[str]:
    return [
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """.strip(),
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.mart_sales_by_products (
            sales_rank int,
            product_key bigint,
            product_name text,
            product_category text,
            total_revenue decimal,
            total_sales_qty bigint,
            avg_rating decimal,
            reviews_count bigint,
            loaded_at timestamp,
            PRIMARY KEY ((sales_rank), product_key)
        )
        """.strip(),
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.mart_sales_by_customers (
            spend_rank int,
            customer_key bigint,
            customer_full_name text,
            customer_country text,
            total_purchase_amount decimal,
            orders_count bigint,
            avg_check decimal,
            loaded_at timestamp,
            PRIMARY KEY ((spend_rank), customer_key)
        )
        """.strip(),
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.mart_sales_by_time (
            year_num int,
            month_num int,
            month_name text,
            total_revenue decimal,
            orders_count bigint,
            avg_order_amount decimal,
            period_start date,
            period_end date,
            loaded_at timestamp,
            PRIMARY KEY ((year_num), month_num)
        )
        """.strip(),
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.mart_sales_by_stores (
            revenue_rank int,
            store_key bigint,
            store_name text,
            store_city text,
            store_country text,
            total_revenue decimal,
            orders_count bigint,
            avg_check decimal,
            loaded_at timestamp,
            PRIMARY KEY ((revenue_rank), store_key)
        )
        """.strip(),
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.mart_sales_by_suppliers (
            revenue_rank int,
            supplier_key bigint,
            supplier_name text,
            supplier_country text,
            total_revenue decimal,
            avg_product_price decimal,
            orders_count bigint,
            loaded_at timestamp,
            PRIMARY KEY ((revenue_rank), supplier_key)
        )
        """.strip(),
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.mart_product_quality (
            product_key bigint PRIMARY KEY,
            product_name text,
            product_category text,
            avg_rating decimal,
            reviews_count bigint,
            sold_quantity bigint,
            rating_sales_corr double,
            quality_label text,
            loaded_at timestamp
        )
        """.strip(),
    ]


def execute_cql_statements(statements: list[str]) -> None:
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    cassandra_host = env("CASSANDRA_HOST", "cassandra")
    cassandra_port = int(env("CASSANDRA_PORT", "9042"))
    cassandra_user = env("CASSANDRA_USER", "")
    cassandra_password = env("CASSANDRA_PASSWORD", "")

    cluster_kwargs: dict = {"contact_points": [cassandra_host], "port": cassandra_port}
    if cassandra_user:
        cluster_kwargs["auth_provider"] = PlainTextAuthProvider(
            username=cassandra_user,
            password=cassandra_password,
        )

    cluster = Cluster(**cluster_kwargs)
    session = cluster.connect()
    try:
        for statement in statements:
            sql = statement.strip()
            if sql:
                session.execute(sql)
    finally:
        cluster.shutdown()


def prepare_schema(keyspace: str, marts: list[CassandraMartSpec]) -> None:
    execute_cql_statements(schema_statements(keyspace))
    truncate_statements = [f"TRUNCATE {keyspace}.{spec.target_table}" for spec in marts]
    execute_cql_statements(truncate_statements)


def build_mart_specs() -> list[CassandraMartSpec]:
    return [
        CassandraMartSpec(
            name="sales_by_products",
            target_table="mart_sales_by_products",
            source_query=source_query("sales_by_products"),
        ),
        CassandraMartSpec(
            name="sales_by_customers",
            target_table="mart_sales_by_customers",
            source_query=source_query("sales_by_customers"),
        ),
        CassandraMartSpec(
            name="sales_by_time",
            target_table="mart_sales_by_time",
            source_query=source_query("sales_by_time"),
        ),
        CassandraMartSpec(
            name="sales_by_stores",
            target_table="mart_sales_by_stores",
            source_query=source_query("sales_by_stores"),
        ),
        CassandraMartSpec(
            name="sales_by_suppliers",
            target_table="mart_sales_by_suppliers",
            source_query=source_query("sales_by_suppliers"),
        ),
        CassandraMartSpec(
            name="product_quality",
            target_table="mart_product_quality",
            source_query=source_query("product_quality"),
        ),
    ]


def create_spark_session() -> SparkSession:
    pg_jdbc_jar = env("POSTGRES_JDBC_JAR", "/home/jovyan/work/jdbc/postgresql-42.7.3.jar")
    cassandra_spark_package = env(
        "CASSANDRA_SPARK_PACKAGE",
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1",
    )

    spark = SparkJob(
        app_name="lab2-star-to-cassandra",
        jdbc_jars=[pg_jdbc_jar],
        spark_packages=[cassandra_spark_package],
    ).start()

    spark.conf.set("spark.cassandra.connection.host", env("CASSANDRA_HOST", "cassandra"))
    spark.conf.set("spark.cassandra.connection.port", env("CASSANDRA_PORT", "9042"))
    spark.conf.set("spark.cassandra.connection.localDC", env("CASSANDRA_LOCAL_DC", "datacenter1"))

    cassandra_user = env("CASSANDRA_USER", "")
    cassandra_password = env("CASSANDRA_PASSWORD", "")
    if cassandra_user:
        spark.conf.set("spark.cassandra.auth.username", cassandra_user)
    if cassandra_password:
        spark.conf.set("spark.cassandra.auth.password", cassandra_password)

    return spark


def prepare_mart_df(df: DataFrame) -> DataFrame:
    return (
        df.na.fill("UNKNOWN")
        .na.fill(0)
        .withColumn("loaded_at", F.current_timestamp())
    )


def write_to_cassandra(df: DataFrame, keyspace: str, table: str) -> None:
    (
        df.write.format("org.apache.spark.sql.cassandra")
        .option("keyspace", keyspace)
        .option("table", table)
        .mode("append")
        .save()
    )


def main() -> None:
    keyspace = env("CASSANDRA_KEYSPACE", "lab2")
    marts = build_mart_specs()
    postgres = JdbcConnection.postgres_from_env()

    prepare_schema(keyspace, marts)
    spark = create_spark_session()
    try:
        for spec in marts:
            print(f"[star_to_cassandra] Building mart: {spec.name}")
            mart_df = postgres.read_query(spark, spec.source_query, alias=spec.name)
            mart_df = prepare_mart_df(mart_df)
            write_to_cassandra(mart_df, keyspace, spec.target_table)
            print(f"[star_to_cassandra] Loaded mart: {keyspace}.{spec.target_table}")
    finally:
        SparkJob.stop(spark)


if __name__ == "__main__":
    main()
