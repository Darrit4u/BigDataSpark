from dataclasses import dataclass


@dataclass(frozen=True)
class SourceMartSpec:
    name: str
    source_query: str


SOURCE_MART_SPECS: list[SourceMartSpec] = [
    SourceMartSpec(
        name="sales_by_products",
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
    SourceMartSpec(
        name="sales_by_customers",
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
    SourceMartSpec(
        name="sales_by_time",
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
        """,
    ),
    SourceMartSpec(
        name="sales_by_stores",
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
        """,
    ),
    SourceMartSpec(
        name="sales_by_suppliers",
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
        """,
    ),
    SourceMartSpec(
        name="product_quality",
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
        """,
    ),
]


def source_query(name: str) -> str:
    for spec in SOURCE_MART_SPECS:
        if spec.name == name:
            return spec.source_query
    raise ValueError(f"Source mart '{name}' is not defined in SOURCE_MART_SPECS")
