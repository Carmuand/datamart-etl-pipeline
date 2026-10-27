CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_type VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_code VARCHAR(50) PRIMARY KEY,
    description_canonical VARCHAR(255) NOT NULL,
    category VARCHAR(100) DEFAULT 'Sin Categoría'
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    product_code VARCHAR(50) REFERENCES dim_product(product_code),
    customer_id VARCHAR(50) REFERENCES dim_customer(customer_id),
    country_id INT REFERENCES dim_country(country_id),
    date_id DATE REFERENCES dim_date(date_id),
    quantity INT NOT NULL,
    unit_price NUMERIC(12, 4) NOT NULL,
    revenue_bruto NUMERIC(12, 4) NOT NULL,
    source_file VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_returns (
    return_id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    product_code VARCHAR(50) REFERENCES dim_product(product_code),
    customer_id VARCHAR(50) REFERENCES dim_customer(customer_id),
    country_id INT REFERENCES dim_country(country_id),
    date_id DATE REFERENCES dim_date(date_id),
    quantity INT NOT NULL,
    unit_price NUMERIC(12, 4) NOT NULL,
    revenue_bruto NUMERIC(12, 4) NOT NULL,
    source_file VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_daily_product_net (
    date_id DATE REFERENCES dim_date(date_id),
    product_code VARCHAR(50) REFERENCES dim_product(product_code),
    net_quantity INT NOT NULL,
    net_revenue NUMERIC(12, 4) NOT NULL,
    PRIMARY KEY (date_id, product_code)
);

CREATE TABLE IF NOT EXISTS rejected_records (
    id SERIAL PRIMARY KEY,
    source_file VARCHAR(100) NOT NULL,
    raw_record_data TEXT NOT NULL,
    rejection_reason VARCHAR(255) NOT NULL,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dim_customer (customer_id, customer_type) VALUES ('GENERICO_DATA_MART', 'ANÓNIMO') ON CONFLICT DO NOTHING;
