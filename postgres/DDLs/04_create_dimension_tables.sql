DROP TABLE IF EXISTS dwh.dim_customer CASCADE;
DROP TABLE IF EXISTS dwh.dim_seller CASCADE;

-- Create customer dimension
CREATE TABLE dwh.dim_customer (
    customer_key SERIAL PRIMARY KEY, -- Surrogate Key
    customer_id VARCHAR(32) NOT NULL, -- Natural/Business Key from source
    customer_unique_id VARCHAR(32) NOT NULL,
    customer_zip_code_prefix VARCHAR(5) NOT NULL,
    customer_city VARCHAR(100) NOT NULL, -- Standardized in ETL
    customer_state VARCHAR(2) NOT NULL, -- Standardized in ETL
    customer_state_name VARCHAR(50) NULL, -- Optional: Populated in ETL
    customer_region VARCHAR(50) NULL, -- Optional: Populated in ETL
    effective_start_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- For SCD Type 2
    effective_end_date TIMESTAMP NULL, -- For SCD Type 2
    is_current BOOLEAN NOT NULL DEFAULT TRUE -- For SCD Type 2
);

-- Indexes for DimCustomer
CREATE INDEX idx_dim_customer_customer_id ON dwh.dim_customer(customer_id); -- Index natural key
CREATE UNIQUE INDEX uidx_dim_customer_id_end_date ON dwh.dim_customer(customer_id, effective_end_date); -- Ensure only one current record per customer
CREATE INDEX idx_dim_customer_unique_id ON dwh.dim_customer(customer_unique_id);
CREATE INDEX idx_dim_customer_state ON dwh.dim_customer(customer_state);
CREATE INDEX idx_dim_customer_city ON dwh.dim_customer(customer_city);
CREATE INDEX idx_dim_customer_is_current ON dwh.dim_customer(is_current);


-- Create seller dimension
CREATE TABLE dwh.dim_seller (
    seller_key SERIAL PRIMARY KEY, -- Surrogate Key
    seller_id VARCHAR(32) NOT NULL, -- Natural/Business Key from source
    seller_zip_code_prefix VARCHAR(5) NOT NULL,
    seller_city VARCHAR(100) NOT NULL, -- Standardized in ETL
    seller_state VARCHAR(2) NOT NULL, -- Standardized in ETL
    seller_state_name VARCHAR(50) NULL, -- Optional: Populated in ETL
    seller_region VARCHAR(50) NULL, -- Optional: Populated in ETL
    effective_start_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- For SCD Type 2
    effective_end_date TIMESTAMP NULL, -- For SCD Type 2
    is_current BOOLEAN NOT NULL DEFAULT TRUE -- For SCD Type 2
);

-- Indexes for DimSeller
CREATE INDEX idx_dim_seller_seller_id ON dwh.dim_seller(seller_id); -- Index natural key
CREATE UNIQUE INDEX uidx_dim_seller_id_end_date ON dwh.dim_seller(seller_id, effective_end_date); -- Ensure only one current record per seller
CREATE INDEX idx_dim_seller_state ON dwh.dim_seller(seller_state);
CREATE INDEX idx_dim_seller_city ON dwh.dim_seller(seller_city);
CREATE INDEX idx_dim_seller_is_current ON dwh.dim_seller(is_current);