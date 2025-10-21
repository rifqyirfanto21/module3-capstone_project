{{
    config(
        materialized='table',
        partition_by={
            "field": "order_date",
            "data_type": "date",
        }
    )
}}

WITH transactions AS(
    SELECT *
    FROM {{ source('rifqy_computerstore_capstone3', 'fct_transactions') }}
)
SELECT
    DATE(created_at) AS order_date,
    COUNT(transaction_id) AS total_orders,
    SUM(total_amount) AS daily_revenue
FROM transactions
GROUP BY 1