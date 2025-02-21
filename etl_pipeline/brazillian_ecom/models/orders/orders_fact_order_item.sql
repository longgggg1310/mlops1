{{
  config(
      schema='orders',
      alias='fact_order_item',
      materialized='table'
  )
}}

WITH orders AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    FROM {{ ref('staging_orders') }}
),

order_items AS (
    SELECT 
        _key AS id,
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        price + freight_value AS cost
    FROM {{ ref('staging_items') }}
),

customer_info AS (
    SELECT 
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix
    FROM {{ ref('staging_order_customer') }}
),

product_info AS (
    SELECT 
        product_id,
        product_category_name
    FROM {{ ref('staging_products') }}
),

seller_info AS (
    SELECT 
        seller_id,
        seller_zip_code_prefix
    FROM {{ ref('staging_seller') }}
)

SELECT 
    oi.id,
    o.order_id,
    oi.order_item_id,
    oi.product_id,
    o.customer_id,
    r.review_id,
    oi.seller_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    pi.product_category_name,
    oi.shipping_limit_date,
    oi.price,
    oi.freight_value,
    oi.cost
FROM orders AS o
LEFT JOIN order_items AS oi ON o.order_id = oi.order_id
LEFT JOIN product_info AS pi ON oi.product_id = pi.product_id
LEFT JOIN {{ ref('staging_reviews') }} AS r ON o.order_id = r.order_id
WHERE oi.order_item_id IS NOT NULL