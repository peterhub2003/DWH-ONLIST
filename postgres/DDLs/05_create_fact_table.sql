DROP TABLE IF EXISTS dwh.fact_order_delivery CASCADE;

-- Create fact table
CREATE TABLE dwh.fact_order_delivery (
    order_delivery_key BIGSERIAL PRIMARY KEY, -- Surrogate Key for Fact
    order_id VARCHAR(32) NOT NULL, -- Degenerate Dimension (Natural key of the order)

    -- Foreign Keys to Dimensions
    purchase_date_key INTEGER NOT NULL,
    approved_date_key INTEGER NULL, -- Can be NULL if not approved yet
    delivered_carrier_date_key INTEGER NULL, -- Can be NULL
    delivered_customer_date_key INTEGER NULL, -- Can be NULL
    estimated_delivery_date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL, -- FK to dim_customer
    seller_key INTEGER NOT NULL, -- FK to dim_seller

    -- Descriptive Attributes from Order
    order_status VARCHAR(20) NOT NULL,

    -- Measures (Calculated in ETL)
    delivery_time_days INTEGER NULL, -- delivered_customer_date - approved_date
    estimated_delivery_time_days INTEGER NULL, -- estimated_delivery_date - approved_date
    delivery_time_difference_days INTEGER NULL, -- delivered_customer_date - estimated_delivery_date
    is_late_delivery_flag BOOLEAN NULL, -- True if delivery_time_difference_days > 0
    time_to_approve_hours NUMERIC(10, 2) NULL, -- approved_at - purchase_timestamp
    seller_processing_hours NUMERIC(10, 2) NULL, -- delivered_carrier_date - approved_at
    carrier_shipping_hours NUMERIC(10, 2) NULL, -- delivered_customer_date - delivered_carrier_date
    item_count INTEGER NOT NULL, -- Aggregated from order_items
    total_freight_value NUMERIC(10, 2) NOT NULL, -- Aggregated from order_items
    total_price NUMERIC(10, 2) NOT NULL, -- Aggregated from order_items (sum of price)
    order_count SMALLINT NOT NULL DEFAULT 1, -- Always 1 for easy counting

    -- Metadata
    dw_load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Key Constraints (using snake_case names)
    CONSTRAINT fk_fod_purchase_date FOREIGN KEY (purchase_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_fod_approved_date FOREIGN KEY (approved_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_fod_delivered_carrier_date FOREIGN KEY (delivered_carrier_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_fod_delivered_customer_date FOREIGN KEY (delivered_customer_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_fod_estimated_date FOREIGN KEY (estimated_delivery_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_fod_customer FOREIGN KEY (customer_key) REFERENCES dwh.dim_customer(customer_key),
    CONSTRAINT fk_fod_seller FOREIGN KEY (seller_key) REFERENCES dwh.dim_seller(seller_key)
);

CREATE UNIQUE INDEX uidx_fod_order_id ON dwh.fact_order_delivery(order_id); -- Ensure one row per order
CREATE INDEX idx_fod_fk_purchase_date ON dwh.fact_order_delivery(purchase_date_key);
CREATE INDEX idx_fod_fk_approved_date ON dwh.fact_order_delivery(approved_date_key);
CREATE INDEX idx_fod_fk_delivered_cust_date ON dwh.fact_order_delivery(delivered_customer_date_key);
CREATE INDEX idx_fod_fk_estimated_date ON dwh.fact_order_delivery(estimated_delivery_date_key);
CREATE INDEX idx_fod_fk_customer ON dwh.fact_order_delivery(customer_key);
CREATE INDEX idx_fod_fk_seller ON dwh.fact_order_delivery(seller_key);
CREATE INDEX idx_fod_status ON dwh.fact_order_delivery(order_status);
CREATE INDEX idx_fod_late_flag ON dwh.fact_order_delivery(is_late_delivery_flag);