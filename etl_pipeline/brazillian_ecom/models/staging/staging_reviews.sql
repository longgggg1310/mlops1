{{ config(
    schema='testing',
    alias='olist_order_reviews_dataset',
    materialized='view'
) }}

WITH ranked_reviews AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY review_answer_timestamp DESC) AS row_num
    FROM {{ source('olist_source', 'olist_order_reviews_dataset') }}
)

SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM ranked_reviews
WHERE row_num = 1
