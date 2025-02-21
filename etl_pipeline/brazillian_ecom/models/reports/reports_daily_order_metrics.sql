{{
    config(
        schema= 'report',
        alias = 'daily report',
        materialized='table'
    )
}}

WITH report_base AS (
    SELECT
        o.order_id,
        oi.order_item_id, -- some order_item_ids aren't available for some orders
        o.order_purchase_timestamp,
        DATE(o.order_purchase_timestamp) AS date,
        o.customer_unique_id,
        o.paid_amount, 
        oi.price,
        oi.product_category_name
    FROM {{ ref('orders_fact_order') }} AS o
    LEFT JOIN {{ ref('orders_fact_order_item') }} AS oi ON o.order_id = oi.order_id
),

daily_metrics AS (
    SELECT
        date,
        COUNT(DISTINCT order_id) AS orders_count,
        COUNT(DISTINCT order_item_id) AS order_items_count,
        ROUND(COUNT(DISTINCT customer_unique_id), 2) AS customers_making_orders_count,
        SUM(price) AS revenue_usd, -- assuming revenue also includes unpaid amounts
        ROUND((SUM(price) / COUNT(DISTINCT order_id))::NUMERIC, 2) AS average_revenue_per_order_usd
    FROM report_base
    GROUP BY date
),

top_3_categories_daily_metrics AS (
    SELECT
        date,
        STRING_AGG(product_category_name, ', ') AS top_3_product_categories_by_revenue,
        STRING_AGG(ROUND(percent_of_total::NUMERIC, 2)::TEXT, ', ') AS top_3_product_categories_revenue_percentage
    FROM (
        SELECT
            report_base.date,
            product_category_name,
            (SUM(price) / MAX(revenue_usd)) * 100 AS percent_of_total, -- using max here since there will be duplicates on the join
            RANK() OVER (PARTITION BY report_base.date ORDER BY SUM(price) DESC) AS category_rank -- rank of categories per day on revenue
        FROM report_base
        LEFT JOIN daily_metrics ON report_base.date = daily_metrics.date
        GROUP BY report_base.date, product_category_name
    ) AS ranked_categories
    WHERE category_rank <= 3
    GROUP BY date
)

SELECT 
    m1.date AS order_purchase_date,
    m1.orders_count,
    m1.customers_making_orders_count,
    m1.revenue_usd, --this could be null for days where orders don't have item_id
    m1.average_revenue_per_order_usd, --this could be null for days where orders don't have item_id
    m2.top_3_product_categories_by_revenue,
    m2.top_3_product_categories_revenue_percentage
FROM daily_metrics AS m1
LEFT JOIN top_3_categories_daily_metrics AS m2 ON m1.date = m2.date
ORDER BY m1.date DESC