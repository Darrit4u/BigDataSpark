from dataclasses import dataclass

from common.jdbc import JdbcConnection
from common.spark_job import SparkJob, env
from source_mart_specs import source_query


@dataclass(frozen=True)
class MartSpec:
    name: str
    target_table: str
    ddl_statements: list[str]
    source_query: str


MART_SPECS: list[MartSpec] = [
    MartSpec(
        name="sales_by_products",
        target_table="lab2.mart_sales_by_products",
        ddl_statements=[
            "DROP TABLE IF EXISTS lab2.mart_sales_by_products",
            """
            CREATE TABLE lab2.mart_sales_by_products
            (
                product_key UInt64,
                product_name String,
                product_category String,
                total_revenue Decimal(18, 2),
                total_sales_qty UInt64,
                sales_rank UInt16,
                avg_rating Decimal(5, 2),
                reviews_count UInt64,
                loaded_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree
            ORDER BY (sales_rank, product_key)
            """,
        ],
        source_query=source_query("sales_by_products"),
    ),
    MartSpec(
        name="sales_by_customers",
        target_table="lab2.mart_sales_by_customers",
        ddl_statements=[
            "DROP TABLE IF EXISTS lab2.mart_sales_by_customers",
            """
            CREATE TABLE lab2.mart_sales_by_customers
            (
                customer_key UInt64,
                customer_full_name String,
                customer_country String,
                total_purchase_amount Decimal(18, 2),
                orders_count UInt64,
                avg_check Decimal(18, 2),
                spend_rank UInt16,
                loaded_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree
            ORDER BY (spend_rank, customer_key)
            """,
        ],
        source_query=source_query("sales_by_customers"),
    ),
    MartSpec(
        name="sales_by_time",
        target_table="lab2.mart_sales_by_time",
        ddl_statements=[
            "DROP TABLE IF EXISTS lab2.mart_sales_by_time",
            """
            CREATE TABLE lab2.mart_sales_by_time
            (
                year_num UInt16,
                month_num UInt8,
                month_name String,
                total_revenue Decimal(18, 2),
                orders_count UInt64,
                avg_order_amount Decimal(18, 2),
                period_start Date,
                period_end Date,
                loaded_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree
            ORDER BY (year_num, month_num)
            """,
        ],
        source_query=source_query("sales_by_time"),
    ),
    MartSpec(
        name="sales_by_stores",
        target_table="lab2.mart_sales_by_stores",
        ddl_statements=[
            "DROP TABLE IF EXISTS lab2.mart_sales_by_stores",
            """
            CREATE TABLE lab2.mart_sales_by_stores
            (
                store_key UInt64,
                store_name String,
                store_city String,
                store_country String,
                total_revenue Decimal(18, 2),
                orders_count UInt64,
                avg_check Decimal(18, 2),
                revenue_rank UInt16,
                loaded_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree
            ORDER BY (revenue_rank, store_key)
            """,
        ],
        source_query=source_query("sales_by_stores"),
    ),
    MartSpec(
        name="sales_by_suppliers",
        target_table="lab2.mart_sales_by_suppliers",
        ddl_statements=[
            "DROP TABLE IF EXISTS lab2.mart_sales_by_suppliers",
            """
            CREATE TABLE lab2.mart_sales_by_suppliers
            (
                supplier_key UInt64,
                supplier_name String,
                supplier_country String,
                total_revenue Decimal(18, 2),
                avg_product_price Decimal(18, 2),
                orders_count UInt64,
                revenue_rank UInt16,
                loaded_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree
            ORDER BY (revenue_rank, supplier_key)
            """,
        ],
        source_query=source_query("sales_by_suppliers"),
    ),
    MartSpec(
        name="product_quality",
        target_table="lab2.mart_product_quality",
        ddl_statements=[
            "DROP TABLE IF EXISTS lab2.mart_product_quality",
            """
            CREATE TABLE lab2.mart_product_quality
            (
                product_key UInt64,
                product_name String,
                product_category String,
                avg_rating Decimal(5, 2),
                reviews_count UInt64,
                sold_quantity UInt64,
                rating_sales_corr Float64,
                quality_label String,
                loaded_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree
            ORDER BY product_key
            """,
        ],
        source_query=source_query("product_quality"),
    ),
]


def main() -> None:
    postgres = JdbcConnection.postgres_from_env()
    clickhouse = JdbcConnection.clickhouse_from_env()

    pg_jdbc_jar = env(
        "POSTGRES_JDBC_JAR",
        "/home/jovyan/work/jdbc/postgresql-42.7.3.jar",
    )
    ch_jdbc_jar = env(
        "CLICKHOUSE_JDBC_JAR",
        "/home/jovyan/work/jdbc/clickhouse-jdbc-0.6.3.jar",
    )

    spark = SparkJob(
        app_name="lab2-star-to-clickhouse",
        jdbc_jars=[pg_jdbc_jar, ch_jdbc_jar],
    ).start()

    try:
        for spec in MART_SPECS:
            clickhouse.execute_sql(spark, spec.ddl_statements)
            mart_df = postgres.read_query(spark, spec.source_query, alias=spec.name)
            mart_df = mart_df.na.fill("UNKNOWN").na.fill(0)
            clickhouse.write_table(mart_df, spec.target_table, mode="append")
    finally:
        SparkJob.stop(spark)


if __name__ == "__main__":
    main()
