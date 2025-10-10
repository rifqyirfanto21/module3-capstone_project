WITH transactions AS(
    SELECT *
    FROM {{ source('rifqy_computerstore_capstone3', 'fct_transactions') }}
)
SELECT
    DATE_TRUNC(created_at, day) AS order_date,
    COUNT(transaction_id) AS total_orders,
    SUM(total_amount) AS daily_revenue
FROM transactions
GROUP BY 1
ORDER BY 1