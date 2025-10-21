{{
    config(
        materialized='table',
        cluster_by=['carrier_name', 'shipping_type']
    )
}}


WITH transactions AS(
    SELECT *
    FROM {{ source('rifqy_computerstore_capstone3', 'fct_transactions') }}
), shipping_methods AS(
    SELECT *
    FROM {{ source('rifqy_computerstore_capstone3', 'dim_shipping_methods') }}
)
SELECT
    sm.carrier_name,
    sm.shipping_type,
    COUNT(t.transaction_id) AS total_transactions,
    SUM(t.total_amount) AS total_revenue
FROM transactions t
JOIN shipping_methods sm ON t.shipping_method_id = sm.shipping_method_id
GROUP BY 1, 2