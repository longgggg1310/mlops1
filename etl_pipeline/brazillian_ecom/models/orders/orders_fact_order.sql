{{
  config(
      schema='orders',
      alias='fact_order',
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
        order_id,
        SUM(price) AS price,
        SUM(freight_value) AS freight_value,
        SUM(cost) AS cost
    FROM {{ ref('orders_fact_order_item') }}
    GROUP BY order_id
),

order_payments AS (
    SELECT 
        order_id,
        SUM(payment_value) AS paid_amount
    FROM {{ ref('staging_payments') }}
    GROUP BY order_id
)

SELECT 
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    r.review_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    COALESCE(oi.price, 0) AS total_price,
    COALESCE(oi.freight_value, 0) AS total_freight,
    COALESCE(oi.cost, 0) AS total_cost,
    COALESCE(op.paid_amount, 0) AS paid_amount
FROM orders AS o
LEFT JOIN {{ ref('staging_reviews') }} AS r ON o.order_id = r.order_id
LEFT JOIN {{ ref('staging_order_customer') }} AS c ON o.customer_id = c.customer_id
LEFT JOIN order_items AS oi ON o.order_id = oi.order_id
LEFT JOIN order_payments AS op ON o.order_id = op.order_id