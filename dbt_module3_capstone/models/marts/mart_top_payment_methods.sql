{{
    config(
        materialized='table',
        cluster_by=['method_name', 'provider']
    )
}}

WITH transactions AS(
    SELECT *
    FROM {{ source('rifqy_computerstore_capstone3', 'fct_transactions') }}
), payment_methods AS(
    SELECT *
    FROM {{ source('rifqy_computerstore_capstone3', 'dim_payment_methods') }}
)
SELECT
    pm.method_name,
    pm.provider,
    COUNT(t.transaction_id) AS total_transactions,
    SUM(t.total_amount) AS total_revenue
FROM transactions t
JOIN payment_methods pm ON t.payment_method_id = pm.payment_method_id
GROUP BY 1, 2