from dataclasses import dataclass

from common.jdbc import JdbcConnection
from common.spark_job import SparkJob, env


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
        source_query="""
        WITH product_sales AS (
            SELECT
                dp.product_key,
                dp.name AS product_name,
                dp.category AS product_category,
                COALESCE(SUM(fs.sale_total_price), 0)::numeric(18,2) AS total_revenue,
                COALESCE(SUM(fs.sale_quantity), 0)::bigint AS total_sales_qty,
                COALESCE(dp.rating, 0)::numeric(5,2) AS avg_rating,
                COALESCE(dp.reviews, 0)::bigint AS reviews_count
            FROM dim_product dp
            LEFT JOIN fact_sales fs ON fs.product_key = dp.product_key
            GROUP BY dp.product_key, dp.name, dp.category, dp.rating, dp.reviews
        ),
        ranked AS (
            SELECT
                *,
                DENSE_RANK() OVER (ORDER BY total_sales_qty DESC, product_key) AS sales_rank
            FROM product_sales
        )
        SELECT
            product_key,
            product_name,
            product_category,
            total_revenue,
            total_sales_qty,
            sales_rank,
            avg_rating,
            reviews_count
        FROM ranked
        ORDER BY sales_rank, product_key
        """,
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
        source_query="""
        WITH customer_sales AS (
            SELECT 
                dc.customer_key,
                dc.first_name || ' ' || dc.last_name AS customer_full_name,
                dc.country AS customer_country,
                COALESCE(SUM(fs.sale_total_price), 0)::numeric(18, 2) AS total_purchase_amount,
                COUNT(fs.sale_key)::bigint AS orders_count,
                COALESCE(SUM(fs.sale_total_price), 0)::numeric(18,2)
                    / NULLIF(COUNT(fs.sale_key), 0)::numeric(18,2) AS avg_check

            FROM dim_customer dc
            LEFT JOIN fact_sales fs ON fs.customer_key = dc.customer_key
            GROUP BY dc.customer_key, dc.first_name, dc.last_name, dc.country
        ),
        ranked AS (
            SELECT
                *,
                DENSE_RANK() OVER (ORDER BY total_purchase_amount DESC, customer_key) AS spend_rank
            FROM customer_sales
        )

        SELECT 
            customer_key,
            customer_full_name,
            customer_country,
            total_purchase_amount,
            orders_count,
            avg_check,
            spend_rank
        FROM ranked
        ORDER BY spend_rank, customer_key
        """,
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
        source_query="""
        WITH monthly_sales AS (
            SELECT
                dd.year_num,
                dd.month_num,
                dd.month_name,
                COALESCE(SUM(fs.sale_total_price), 0)::numeric(18,2) AS total_revenue,
                COUNT(fs.sale_key)::bigint AS orders_count,
                COALESCE(SUM(fs.sale_total_price), 0)::numeric(18,2)
                    / NULLIF(COUNT(fs.sale_key), 0)::numeric(18,2) AS avg_order_amount,
                MIN(dd.full_date) AS period_start,
                MAX(dd.full_date) AS period_end
            FROM dim_date dd
            LEFT JOIN fact_sales fs
                ON fs.date_key = dd.date_key
            WHERE dd.date_key <> 0
            AND dd.full_date IS NOT NULL
            GROUP BY dd.year_num, dd.month_num, dd.month_name
        )
        SELECT
            year_num,
            month_num,
            month_name,
            total_revenue,
            orders_count,
            COALESCE(avg_order_amount, 0)::numeric(18,2) AS avg_order_amount,
            period_start,
            period_end
        FROM monthly_sales
        ORDER BY year_num, month_num
        """
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
        source_query="""
        WITH store_sales AS (
            SELECT
                ds.store_key,
                ds.name AS store_name,
                ds.city AS store_city,
                ds.country AS store_country,
                COALESCE(SUM(fs.sale_total_price), 0)::numeric(18,2) AS total_revenue,
                COUNT(fs.sale_key)::bigint AS orders_count,
                COALESCE(SUM(fs.sale_total_price), 0)::numeric(18,2)
                    / NULLIF(COUNT(fs.sale_key), 0)::numeric(18,2) AS avg_check
            FROM dim_store ds
            LEFT JOIN fact_sales fs
                ON fs.store_key = ds.store_key
            GROUP BY ds.store_key, ds.name, ds.city, ds.country
        ),
        ranked AS (
            SELECT
                *,
                DENSE_RANK() OVER (ORDER BY total_revenue DESC, store_key) AS revenue_rank
            FROM store_sales
        )
        SELECT
            store_key,
            store_name,
            store_city,
            store_country,
            total_revenue,
            orders_count,
            COALESCE(avg_check, 0)::numeric(18,2) AS avg_check,
            revenue_rank
        FROM ranked
        ORDER BY revenue_rank, store_key
        """
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
        source_query="""
        WITH supplier_sales AS (
            SELECT
                dsp.supplier_key,
                dsp.name AS supplier_name,
                dsp.country AS supplier_country,
                COALESCE(SUM(fs.sale_total_price), 0)::numeric(18,2) AS total_revenue,
                COALESCE(AVG(dp.price), 0)::numeric(18,2) AS avg_product_price,
                COUNT(fs.sale_key)::bigint AS orders_count
            FROM dim_supplier dsp
            LEFT JOIN fact_sales fs
                ON fs.supplier_key = dsp.supplier_key
            LEFT JOIN dim_product dp
                ON dp.product_key = fs.product_key
            GROUP BY dsp.supplier_key, dsp.name, dsp.country
        ),
        ranked AS (
            SELECT
                *,
                DENSE_RANK() OVER (ORDER BY total_revenue DESC, supplier_key) AS revenue_rank
            FROM supplier_sales
        )
        SELECT
            supplier_key,
            supplier_name,
            supplier_country,
            total_revenue,
            avg_product_price,
            orders_count,
            revenue_rank
        FROM ranked
        ORDER BY revenue_rank, supplier_key
        """
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
        source_query="""
        WITH product_sales AS (
            SELECT
                dp.product_key,
                dp.name AS product_name,
                dp.category AS product_category,
                COALESCE(dp.rating, 0)::numeric(5,2) AS avg_rating,
                COALESCE(dp.reviews, 0)::bigint AS reviews_count,
                COALESCE(SUM(fs.sale_quantity), 0)::bigint AS sold_quantity
            FROM dim_product dp
            LEFT JOIN fact_sales fs
                ON fs.product_key = dp.product_key
            GROUP BY dp.product_key, dp.name, dp.category, dp.rating, dp.reviews
        ),
        extremes AS (
            SELECT
                MAX(avg_rating) AS max_rating,
                MIN(avg_rating) AS min_rating,
                MAX(reviews_count) AS max_reviews
            FROM product_sales
        ),
        corr_cte AS (
            SELECT
                COALESCE(
                    corr(avg_rating::double precision, sold_quantity::double precision),
                    0
                )::double precision AS rating_sales_corr
            FROM product_sales
        )
        SELECT
            ps.product_key,
            ps.product_name,
            ps.product_category,
            ps.avg_rating,
            ps.reviews_count,
            ps.sold_quantity,
            cc.rating_sales_corr,
            CONCAT_WS(
                ',',
                CASE WHEN ps.avg_rating = e.max_rating THEN 'HIGHEST_RATING' END,
                CASE WHEN ps.avg_rating = e.min_rating THEN 'LOWEST_RATING' END,
                CASE WHEN ps.reviews_count = e.max_reviews THEN 'MOST_REVIEWS' END
            ) AS quality_label
        FROM product_sales ps
        CROSS JOIN extremes e
        CROSS JOIN corr_cte cc
        ORDER BY ps.avg_rating DESC, ps.reviews_count DESC, ps.product_key
        """
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
