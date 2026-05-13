CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key BIGSERIAL PRIMARY KEY,
    source_customer_id BIGINT UNIQUE,
    first_name TEXT,
    last_name TEXT,
    age INT,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT,
    pet_category TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_key BIGSERIAL PRIMARY KEY,
    source_seller_id BIGINT UNIQUE,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key BIGSERIAL PRIMARY KEY,
    source_product_id BIGINT UNIQUE,
    name TEXT,
    category TEXT,
    price NUMERIC(12,2),
    stock_quantity INT,
    weight NUMERIC(10,2),
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(3,1),
    reviews INT,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key BIGSERIAL PRIMARY KEY,
    name TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT,
    UNIQUE (name, phone)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key BIGSERIAL PRIMARY KEY,
    name TEXT,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT,
    UNIQUE (name, email)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE UNIQUE,
    day_of_month INT,
    month_num INT,
    month_name TEXT,
    quarter_num INT,
    year_num INT,
    week_of_year INT,
    day_of_week INT,
    day_name TEXT,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_key BIGSERIAL PRIMARY KEY,
    source_sale_id BIGINT,
    customer_key BIGINT NOT NULL REFERENCES dim_customer(customer_key),
    seller_key BIGINT NOT NULL REFERENCES dim_seller(seller_key),
    product_key BIGINT NOT NULL REFERENCES dim_product(product_key),
    store_key BIGINT NOT NULL REFERENCES dim_store(store_key),
    supplier_key BIGINT NOT NULL REFERENCES dim_supplier(supplier_key),
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    sale_quantity INT,
    sale_total_price NUMERIC(12,2)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(date_key);
