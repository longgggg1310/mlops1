{{
  config(
      schema='testing',
      alias='olist_customers_dataset',
      materialized='view'
  )
}}

SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('olist_source', 'olist_customers_dataset') }}