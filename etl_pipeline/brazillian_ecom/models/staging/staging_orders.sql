{{
  config(
      schema='testing',
      alias='olist_orders_dataset',
      materialized='view'
  )
}}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date


FROM {{ source('olist_source', 'olist_orders_dataset') }}