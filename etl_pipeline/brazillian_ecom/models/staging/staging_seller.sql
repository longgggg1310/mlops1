{{
  config(
      schema='testing',
      alias='olist_sellers_dataset',
      materialized='view'
  )
}}

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state


FROM {{ source('olist_source', 'olist_sellers_dataset') }}