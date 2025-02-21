CREATE DATABASE IF NOT EXISTS testing;

CREATE SCHEMA IF NOT EXISTS testing;

DROP TABLE IF EXISTS testing.olist_order_items_dataset;
DROP TABLE IF EXISTS testing.olist_products_dataset;
DROP TABLE IF EXISTS testing.olist_orders_dataset;
DROP TABLE IF EXISTS testing.olist_order_payments_dataset;
DROP TABLE IF EXISTS testing.olist_sellers_dataset;
DROP TABLE IF EXISTS testing.olist_customers_dataset;
DROP TABLE IF EXISTS testing.olist_geolocation_dataset;
DROP TABLE IF EXISTS testing.olist_order_reviews_dataset;

CREATE TABLE testing.olist_geolocation_dataset (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat NUMERIC(10, 8),
    geolocation_lng NUMERIC(11, 8),
    geolocation_city VARCHAR(100),
    geolocation_state CHAR(2)
);

CREATE TABLE testing.olist_orders_dataset (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE testing.olist_customers_dataset (
    customer_id CHAR(32),
    customer_unique_id CHAR(32),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state CHAR(2)
);

CREATE TABLE testing.olist_order_items_dataset (
    order_id VARCHAR(32),
    order_item_id INT,
    product_id VARCHAR(32),
    seller_id VARCHAR(32),
    shipping_limit_date TIMESTAMP,
    price REAL,  -- FLOAT in MySQL is equivalent to REAL in PostgreSQL
    freight_value REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE testing.olist_order_payments_dataset (
    order_id VARCHAR(32),
    payment_sequential INT,
    payment_type VARCHAR(16),
    payment_installments INT,
    payment_value REAL
);

CREATE TABLE testing.olist_order_reviews_dataset (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INT NOT NULL,   
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP
);

CREATE TABLE testing.olist_products_dataset (
    product_id VARCHAR(32),
    product_category_name VARCHAR(64),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

CREATE TABLE testing.olist_sellers_dataset (
    seller_id CHAR(32),
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state CHAR(2)
);