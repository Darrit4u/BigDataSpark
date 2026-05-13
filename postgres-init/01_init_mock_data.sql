-- 01_init_mock_data.sql
-- Пункт 6: создать таблицу mock_data и загрузить все CSV-файлы.

DROP TABLE IF EXISTS mock_data;

CREATE TABLE mock_data (
    id BIGINT,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INT,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC(12,2),
    product_quantity INT,
    sale_date TEXT,
    sale_customer_id BIGINT,
    sale_seller_id BIGINT,
    sale_product_id BIGINT,
    sale_quantity INT,
    sale_total_price NUMERIC(12,2),
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    pet_category TEXT,
    product_weight NUMERIC(10,2),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(3,1),
    product_reviews INT,
    product_release_date TEXT,
    product_expiry_date TEXT,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

\copy mock_data FROM '/csv/MOCK_DATA.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy mock_data FROM '/csv/MOCK_DATA (1).csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy mock_data FROM '/csv/MOCK_DATA (2).csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy mock_data FROM '/csv/MOCK_DATA (3).csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy mock_data FROM '/csv/MOCK_DATA (4).csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy mock_data FROM '/csv/MOCK_DATA (5).csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy mock_data FROM '/csv/MOCK_DATA (6).csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy mock_data FROM '/csv/MOCK_DATA (7).csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy mock_data FROM '/csv/MOCK_DATA (8).csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy mock_data FROM '/csv/MOCK_DATA (9).csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

DO $$
DECLARE
    row_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM mock_data;
    RAISE NOTICE 'mock_data rows loaded: %', row_count;
END $$;
