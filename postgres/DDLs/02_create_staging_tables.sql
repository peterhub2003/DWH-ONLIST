DROP TABLE IF EXISTS staging.stg_orders CASCADE;
DROP TABLE IF EXISTS staging.stg_order_items CASCADE;
DROP TABLE IF EXISTS staging.stg_customers CASCADE;
DROP TABLE IF EXISTS staging.stg_sellers CASCADE;
DROP TABLE IF EXISTS staging.stg_geolocation CASCADE;

CREATE TABLE staging.stg_orders (
    order_id VARCHAR(32),
    customer_id VARCHAR(32),
    order_status VARCHAR(20),
    order_purchase_timestamp VARCHAR(30),
    order_approved_at VARCHAR(30),
    order_delivered_carrier_date VARCHAR(30),
    order_delivered_customer_date VARCHAR(30),
    order_estimated_delivery_date VARCHAR(30),
    _load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.stg_order_items (
    order_id VARCHAR(32),
    order_item_id VARCHAR(5),
    product_id VARCHAR(32),
    seller_id VARCHAR(32),
    shipping_limit_date VARCHAR(30),
    price VARCHAR(20),
    freight_value VARCHAR(20),
    _load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.stg_customers (
    customer_id VARCHAR(32),
    customer_unique_id VARCHAR(32),
    customer_zip_code_prefix VARCHAR(5),
    customer_city VARCHAR(100),
    customer_state VARCHAR(2),
    _load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.stg_sellers (
    seller_id VARCHAR(32),
    seller_zip_code_prefix VARCHAR(5),
    seller_city VARCHAR(100),
    seller_state VARCHAR(2),
    _load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.stg_geolocation (
    geolocation_zip_code_prefix VARCHAR(5),
    geolocation_lat VARCHAR(30),
    geolocation_lng VARCHAR(30),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(2),
    _load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_orders_order_id ON staging.stg_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_items_order_id ON staging.stg_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_items_seller_id ON staging.stg_order_items(seller_id);
CREATE INDEX IF NOT EXISTS idx_stg_customers_customer_id ON staging.stg_customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_stg_sellers_seller_id ON staging.stg_sellers(seller_id);
CREATE INDEX IF NOT EXISTS idx_stg_geolocation_zip_prefix ON staging.stg_geolocation(geolocation_zip_code_prefix);