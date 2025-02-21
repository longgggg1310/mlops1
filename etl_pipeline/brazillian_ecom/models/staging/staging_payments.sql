{{
  config(
      schema='testing',
      alias='olist_order_payments_dataset',
      materialized='view'
  )
}}

SELECT
    CONCAT(order_id, '-', payment_sequential) AS _key,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value


FROM {{ source('olist_source', 'olist_order_payments_dataset') }}