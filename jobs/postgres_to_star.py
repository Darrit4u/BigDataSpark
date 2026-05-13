from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from common.jdbc import JdbcConnection
from common.spark_job import SparkJob, env


TRUNCATE_SQL = [
    "ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fact_sales_source_sale_id_key;",
    "TRUNCATE TABLE fact_sales, dim_customer, dim_seller, dim_product, dim_store, dim_supplier, dim_date RESTART IDENTITY CASCADE;",
]


def normalize_text(col_name: str):
    col = F.trim(F.col(col_name))
    return F.when(col == "", F.lit(None)).otherwise(col)


def create_spark_session() -> SparkSession:
    pg_jdbc_jar = env(
        "POSTGRES_JDBC_JAR",
        "/home/jovyan/work/jdbc/postgresql-42.7.3.jar",
    )
    return SparkJob(
        app_name="lab2-postgres-to-star",
        jdbc_jar_path=pg_jdbc_jar,
    ).start()


def prepare_target_tables(pg: JdbcConnection, spark: SparkSession) -> None:
    pg.execute_sql(spark, TRUNCATE_SQL)


def load_mock_data(pg: JdbcConnection, spark: SparkSession) -> DataFrame:
    return pg.read_table(spark, "mock_data")


def prepare_base_dataframe(src: DataFrame) -> DataFrame:
    return (
        src.select(
            F.col("id").cast("long").alias("id"),
            F.col("sale_customer_id").cast("long").alias("source_customer_id"),
            F.col("sale_seller_id").cast("long").alias("source_seller_id"),
            F.col("sale_product_id").cast("long").alias("source_product_id"),
            F.col("customer_first_name"),
            F.col("customer_last_name"),
            F.col("customer_age").cast("int").alias("customer_age"),
            F.col("customer_email"),
            F.col("customer_country"),
            F.col("customer_postal_code"),
            F.col("customer_pet_type"),
            F.col("customer_pet_name"),
            F.col("customer_pet_breed"),
            F.col("pet_category"),
            F.col("seller_first_name"),
            F.col("seller_last_name"),
            F.col("seller_email"),
            F.col("seller_country"),
            F.col("seller_postal_code"),
            F.col("product_name"),
            F.col("product_category"),
            F.col("product_price").cast("decimal(12,2)").alias("product_price"),
            F.col("product_quantity").cast("int").alias("product_quantity"),
            F.col("product_weight").cast("decimal(10,2)").alias("product_weight"),
            F.col("product_color"),
            F.col("product_size"),
            F.col("product_brand"),
            F.col("product_material"),
            F.col("product_description"),
            F.col("product_rating").cast("decimal(3,1)").alias("product_rating"),
            F.col("product_reviews").cast("int").alias("product_reviews"),
            F.to_date(F.col("product_release_date"), "M/d/yyyy").alias("product_release_date"),
            F.to_date(F.col("product_expiry_date"), "M/d/yyyy").alias("product_expiry_date"),
            F.col("store_name"),
            F.col("store_location"),
            F.col("store_city"),
            F.col("store_state"),
            F.col("store_country"),
            F.col("store_phone"),
            F.col("store_email"),
            F.col("supplier_name"),
            F.col("supplier_contact"),
            F.col("supplier_email"),
            F.col("supplier_phone"),
            F.col("supplier_address"),
            F.col("supplier_city"),
            F.col("supplier_country"),
            F.col("sale_quantity").cast("int").alias("sale_quantity"),
            F.col("sale_total_price").cast("decimal(12,2)").alias("sale_total_price"),
            F.to_date(F.col("sale_date"), "M/d/yyyy").alias("sale_date"),
        )
        .withColumn("store_name_key", F.coalesce(normalize_text("store_name"), F.lit("UNKNOWN")))
        .withColumn("store_phone_key", F.coalesce(normalize_text("store_phone"), F.lit("UNKNOWN")))
        .withColumn("supplier_name_key", F.coalesce(normalize_text("supplier_name"), F.lit("UNKNOWN")))
        .withColumn("supplier_email_key", F.coalesce(normalize_text("supplier_email"), F.lit("UNKNOWN")))
    )


def latest_by_id(df: DataFrame, id_col: str) -> DataFrame:
    w = Window.partitionBy(id_col).orderBy(F.col("sale_date").desc_nulls_last(), F.col("id").desc())
    return df.where(F.col(id_col).isNotNull()).withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")


def build_dim_customer(spark: SparkSession, base: DataFrame) -> DataFrame:
    real = latest_by_id(base, "source_customer_id").select(
        F.row_number().over(Window.orderBy("source_customer_id")).cast("long").alias("customer_key"),
        F.col("source_customer_id"),
        normalize_text("customer_first_name").alias("first_name"),
        normalize_text("customer_last_name").alias("last_name"),
        F.col("customer_age").alias("age"),
        normalize_text("customer_email").alias("email"),
        normalize_text("customer_country").alias("country"),
        normalize_text("customer_postal_code").alias("postal_code"),
        normalize_text("customer_pet_type").alias("pet_type"),
        normalize_text("customer_pet_name").alias("pet_name"),
        normalize_text("customer_pet_breed").alias("pet_breed"),
        normalize_text("pet_category").alias("pet_category"),
    )
    unknown = spark.createDataFrame(
        [(0, None, "UNKNOWN", "UNKNOWN", None, None, None, None, None, None, None, None)],
        "customer_key long, source_customer_id long, first_name string, last_name string, age int, email string, country string, postal_code string, pet_type string, pet_name string, pet_breed string, pet_category string",
    )
    return unknown.unionByName(real)


def build_dim_seller(spark: SparkSession, base: DataFrame) -> DataFrame:
    real = latest_by_id(base, "source_seller_id").select(
        F.row_number().over(Window.orderBy("source_seller_id")).cast("long").alias("seller_key"),
        F.col("source_seller_id"),
        normalize_text("seller_first_name").alias("first_name"),
        normalize_text("seller_last_name").alias("last_name"),
        normalize_text("seller_email").alias("email"),
        normalize_text("seller_country").alias("country"),
        normalize_text("seller_postal_code").alias("postal_code"),
    )
    unknown = spark.createDataFrame(
        [(0, None, "UNKNOWN", "UNKNOWN", None, None, None)],
        "seller_key long, source_seller_id long, first_name string, last_name string, email string, country string, postal_code string",
    )
    return unknown.unionByName(real)


def build_dim_product(spark: SparkSession, base: DataFrame) -> DataFrame:
    real = latest_by_id(base, "source_product_id").select(
        F.row_number().over(Window.orderBy("source_product_id")).cast("long").alias("product_key"),
        F.col("source_product_id"),
        normalize_text("product_name").alias("name"),
        normalize_text("product_category").alias("category"),
        F.col("product_price").alias("price"),
        F.col("product_quantity").alias("stock_quantity"),
        F.col("product_weight").alias("weight"),
        normalize_text("product_color").alias("color"),
        normalize_text("product_size").alias("size"),
        normalize_text("product_brand").alias("brand"),
        normalize_text("product_material").alias("material"),
        normalize_text("product_description").alias("description"),
        F.col("product_rating").alias("rating"),
        F.col("product_reviews").alias("reviews"),
        F.col("product_release_date").alias("release_date"),
        F.col("product_expiry_date").alias("expiry_date"),
    )
    unknown = spark.createDataFrame(
        [(0, None, "UNKNOWN", "UNKNOWN", None, None, None, None, None, None, None, None, None, None, None, None)],
        "product_key long, source_product_id long, name string, category string, price decimal(12,2), stock_quantity int, weight decimal(10,2), color string, size string, brand string, material string, description string, rating decimal(3,1), reviews int, release_date date, expiry_date date",
    )
    return unknown.unionByName(real)


def build_dim_store(spark: SparkSession, base: DataFrame) -> DataFrame:
    real = (
        base.groupBy("store_name_key", "store_phone_key")
        .agg(
            F.first(normalize_text("store_name"), ignorenulls=True).alias("name"),
            F.first(normalize_text("store_location"), ignorenulls=True).alias("location"),
            F.first(normalize_text("store_city"), ignorenulls=True).alias("city"),
            F.first(normalize_text("store_state"), ignorenulls=True).alias("state"),
            F.first(normalize_text("store_country"), ignorenulls=True).alias("country"),
            F.first(normalize_text("store_phone"), ignorenulls=True).alias("phone"),
            F.first(normalize_text("store_email"), ignorenulls=True).alias("email"),
        )
        .where(~((F.col("store_name_key") == "UNKNOWN") & (F.col("store_phone_key") == "UNKNOWN")))
        .withColumn("store_key", F.row_number().over(Window.orderBy("store_name_key", "store_phone_key")).cast("long"))
        .select("store_key", "name", "location", "city", "state", "country", "phone", "email", "store_name_key", "store_phone_key")
    )
    unknown = spark.createDataFrame(
        [(0, "UNKNOWN", None, None, None, None, "UNKNOWN", None, "UNKNOWN", "UNKNOWN")],
        "store_key long, name string, location string, city string, state string, country string, phone string, email string, store_name_key string, store_phone_key string",
    )
    return unknown.unionByName(real)


def build_dim_supplier(spark: SparkSession, base: DataFrame) -> DataFrame:
    real = (
        base.groupBy("supplier_name_key", "supplier_email_key")
        .agg(
            F.first(normalize_text("supplier_name"), ignorenulls=True).alias("name"),
            F.first(normalize_text("supplier_contact"), ignorenulls=True).alias("contact"),
            F.first(normalize_text("supplier_email"), ignorenulls=True).alias("email"),
            F.first(normalize_text("supplier_phone"), ignorenulls=True).alias("phone"),
            F.first(normalize_text("supplier_address"), ignorenulls=True).alias("address"),
            F.first(normalize_text("supplier_city"), ignorenulls=True).alias("city"),
            F.first(normalize_text("supplier_country"), ignorenulls=True).alias("country"),
        )
        .where(~((F.col("supplier_name_key") == "UNKNOWN") & (F.col("supplier_email_key") == "UNKNOWN")))
        .withColumn("supplier_key", F.row_number().over(Window.orderBy("supplier_name_key", "supplier_email_key")).cast("long"))
        .select("supplier_key", "name", "contact", "email", "phone", "address", "city", "country", "supplier_name_key", "supplier_email_key")
    )
    unknown = spark.createDataFrame(
        [(0, "UNKNOWN", None, "UNKNOWN", None, None, None, None, "UNKNOWN", "UNKNOWN")],
        "supplier_key long, name string, contact string, email string, phone string, address string, city string, country string, supplier_name_key string, supplier_email_key string",
    )
    return unknown.unionByName(real)


def build_dim_date(spark: SparkSession, base: DataFrame) -> DataFrame:
    real = (
        base.select(F.col("sale_date").alias("full_date"))
        .where(F.col("full_date").isNotNull())
        .distinct()
        .select(
            F.date_format(F.col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
            F.col("full_date"),
            F.dayofmonth("full_date").alias("day_of_month"),
            F.month("full_date").alias("month_num"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.quarter("full_date").alias("quarter_num"),
            F.year("full_date").alias("year_num"),
            F.weekofyear("full_date").alias("week_of_year"),
            F.dayofweek("full_date").alias("day_of_week"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        )
    )
    unknown = spark.createDataFrame(
        [(0, None, None, None, None, None, None, None, None, None, None)],
        "date_key int, full_date date, day_of_month int, month_num int, month_name string, quarter_num int, year_num int, week_of_year int, day_of_week int, day_name string, is_weekend boolean",
    )
    return unknown.unionByName(real)


def build_dimensions(spark: SparkSession, base: DataFrame) -> dict[str, DataFrame]:
    return {
        "dim_customer": build_dim_customer(spark, base),
        "dim_seller": build_dim_seller(spark, base),
        "dim_product": build_dim_product(spark, base),
        "dim_store": build_dim_store(spark, base),
        "dim_supplier": build_dim_supplier(spark, base),
        "dim_date": build_dim_date(spark, base),
    }


def write_dimensions(pg: JdbcConnection, dims: dict[str, DataFrame]) -> None:
    pg.write_table(dims["dim_customer"], "dim_customer", mode="append")
    pg.write_table(dims["dim_seller"], "dim_seller", mode="append")
    pg.write_table(dims["dim_product"], "dim_product", mode="append")
    pg.write_table(
        dims["dim_store"].select("store_key", "name", "location", "city", "state", "country", "phone", "email"),
        "dim_store",
        mode="append",
    )
    pg.write_table(
        dims["dim_supplier"].select("supplier_key", "name", "contact", "email", "phone", "address", "city", "country"),
        "dim_supplier",
        mode="append",
    )
    pg.write_table(dims["dim_date"], "dim_date", mode="append")


def build_fact_sales(base: DataFrame, dims: dict[str, DataFrame]) -> DataFrame:
    return (
        base.alias("b")
        .join(
            dims["dim_customer"].select("customer_key", "source_customer_id").alias("dc"),
            F.col("b.source_customer_id") == F.col("dc.source_customer_id"),
            "left",
        )
        .join(
            dims["dim_seller"].select("seller_key", "source_seller_id").alias("ds"),
            F.col("b.source_seller_id") == F.col("ds.source_seller_id"),
            "left",
        )
        .join(
            dims["dim_product"].select("product_key", "source_product_id").alias("dp"),
            F.col("b.source_product_id") == F.col("dp.source_product_id"),
            "left",
        )
        .join(
            dims["dim_store"].select("store_key", "store_name_key", "store_phone_key").alias("dst"),
            [
                F.col("b.store_name_key") == F.col("dst.store_name_key"),
                F.col("b.store_phone_key") == F.col("dst.store_phone_key"),
            ],
            "left",
        )
        .join(
            dims["dim_supplier"].select("supplier_key", "supplier_name_key", "supplier_email_key").alias("dsp"),
            [
                F.col("b.supplier_name_key") == F.col("dsp.supplier_name_key"),
                F.col("b.supplier_email_key") == F.col("dsp.supplier_email_key"),
            ],
            "left",
        )
        .join(
            dims["dim_date"].select("date_key", "full_date").alias("dd"),
            F.col("b.sale_date") == F.col("dd.full_date"),
            "left",
        )
        .select(
            F.col("b.id").cast("long").alias("source_sale_id"),
            F.coalesce(F.col("dc.customer_key"), F.lit(0)).cast("long").alias("customer_key"),
            F.coalesce(F.col("ds.seller_key"), F.lit(0)).cast("long").alias("seller_key"),
            F.coalesce(F.col("dp.product_key"), F.lit(0)).cast("long").alias("product_key"),
            F.coalesce(F.col("dst.store_key"), F.lit(0)).cast("long").alias("store_key"),
            F.coalesce(F.col("dsp.supplier_key"), F.lit(0)).cast("long").alias("supplier_key"),
            F.coalesce(F.col("dd.date_key"), F.lit(0)).cast("int").alias("date_key"),
            F.col("b.sale_quantity").cast("int").alias("sale_quantity"),
            F.col("b.sale_total_price").cast("decimal(12,2)").alias("sale_total_price"),
        )
    )


def write_fact_sales(pg: JdbcConnection, fact: DataFrame) -> None:
    pg.write_table(fact, "fact_sales", mode="append")


def print_loaded_counts(pg: JdbcConnection, spark: SparkSession) -> None:
    tables = ["dim_customer", "dim_seller", "dim_product", "dim_store", "dim_supplier", "dim_date", "fact_sales"]
    print("Loaded rows:")
    for table in tables:
        cnt = pg.read_query(spark, f"SELECT COUNT(*) AS c FROM {table}", alias=f"{table}_count").collect()[0]["c"]
        print(f"  {table}: {cnt}")


def main() -> None:
    spark = create_spark_session()
    pg = JdbcConnection.postgres_from_env()

    try:
        prepare_target_tables(pg, spark)
        src = load_mock_data(pg, spark)
        base = prepare_base_dataframe(src)
        dims = build_dimensions(spark, base)
        write_dimensions(pg, dims)
        fact = build_fact_sales(base, dims)
        write_fact_sales(pg, fact)
        print_loaded_counts(pg, spark)
    finally:
        SparkJob.stop(spark)


if __name__ == "__main__":
    main()
