-- Клиенты
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_age INTEGER,
    customer_email VARCHAR(255),
    customer_country VARCHAR(100),
    customer_postal_code VARCHAR(20),
    customer_pet_type VARCHAR(50),
    customer_pet_name VARCHAR(100),
    customer_pet_breed VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Продавцы
CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id SERIAL PRIMARY KEY,
    seller_first_name VARCHAR(100),
    seller_last_name VARCHAR(100),
    seller_email VARCHAR(255),
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Продукты
CREATE TABLE IF NOT EXISTS dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    product_price DECIMAL(10, 2),
    product_weight DECIMAL(10, 2),
    product_color VARCHAR(50),
    product_size VARCHAR(20),
    product_brand VARCHAR(100),
    product_material VARCHAR(100),
    product_description TEXT,
    product_rating DECIMAL(3, 2),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE,
    pet_category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Магазины
CREATE TABLE IF NOT EXISTS dim_store (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(255),
    store_location VARCHAR(255),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_country VARCHAR(100),
    store_phone VARCHAR(50),
    store_email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Поставщики
CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(255),
    supplier_contact VARCHAR(255),
    supplier_email VARCHAR(255),
    supplier_phone VARCHAR(50),
    supplier_address VARCHAR(255),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Дата
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    quarter INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    is_weekend BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Продажи
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_key INTEGER REFERENCES dim_customer(customer_id),
    seller_key INTEGER REFERENCES dim_seller(seller_id),
    product_key INTEGER REFERENCES dim_product(product_id),
    store_key INTEGER REFERENCES dim_store(store_id),
    supplier_key INTEGER REFERENCES dim_supplier(supplier_id),
    date_key INTEGER REFERENCES dim_date(date_id),
    sale_quantity INTEGER,
    sale_total_price DECIMAL(10, 2),
    product_quantity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации запросов
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_seller ON fact_sales(seller_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_key);
CREATE INDEX idx_fact_sales_supplier ON fact_sales(supplier_key);
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);

-- представления для аналитики
CREATE OR REPLACE VIEW v_sales_analytics AS
SELECT 
    fs.sale_id,
    dc.customer_first_name || ' ' || dc.customer_last_name AS customer_name,
    dc.customer_country AS customer_country,
    ds.seller_first_name || ' ' || ds.seller_last_name AS seller_name,
    dp.product_name,
    dp.product_category,
    dp.product_brand,
    dst.store_name,
    dst.store_city,
    dst.store_country,
    dd.full_date AS sale_date,
    dd.year AS sale_year,
    dd.month AS sale_month,
    dd.quarter AS sale_quarter,
    fs.sale_quantity,
    fs.sale_total_price,
    dp.product_price,
    dsu.supplier_name
FROM fact_sales fs
LEFT JOIN dim_customer dc ON fs.customer_key = dc.customer_id
LEFT JOIN dim_seller ds ON fs.seller_key = ds.seller_id
LEFT JOIN dim_product dp ON fs.product_key = dp.product_id
LEFT JOIN dim_store dst ON fs.store_key = dst.store_id
LEFT JOIN dim_supplier dsu ON fs.supplier_key = dsu.supplier_id
LEFT JOIN dim_date dd ON fs.date_key = dd.date_id;

COMMENT ON TABLE dim_customer IS 'Dimension table for customers';
COMMENT ON TABLE dim_seller IS 'Dimension table for sellers';
COMMENT ON TABLE dim_product IS 'Dimension table for products';
COMMENT ON TABLE dim_store IS 'Dimension table for stores';
COMMENT ON TABLE dim_supplier IS 'Dimension table for suppliers';
COMMENT ON TABLE dim_date IS 'Dimension table for dates';
COMMENT ON TABLE fact_sales IS 'Fact table for sales transactions';
