{{
    config(
        materialized='table',
        cluster_by=['brand', 'category']
    )
}}

WITH transactions AS(
    SELECT *
    FROM {{ source('rifqy_computerstore_capstone3', 'fct_transactions') }}
), products AS(
    SELECT *
    FROM {{ source('rifqy_computerstore_capstone3', 'dim_products') }}
)
SELECT
    p.product_id,
    p.product_name,
    p.brand,
    p.category,
    SUM(t.quantity) AS total_quantity_sold,
    SUM(t.total_amount) AS total_revenue,
    SUM((p.price - p.cost) * t.quantity) AS total_profit
FROM transactions t
JOIN products p ON t.product_id = p.product_id
GROUP BY 1, 2, 3, 4