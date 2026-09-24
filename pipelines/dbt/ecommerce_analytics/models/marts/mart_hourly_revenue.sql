{{ config(materialized='table') }}

WITH hourly_transactions AS (
    SELECT
        event_hour,
        COUNT(DISTINCT CASE WHEN event_type = 'payment_completed' THEN order_id END) AS completed_orders,
        COALESCE(SUM(CASE WHEN event_type = 'payment_completed' THEN total_amount ELSE 0 END), 0) AS total_gmv,
        COUNT(DISTINCT CASE WHEN event_type = 'order_cancelled' THEN order_id END) AS cancelled_orders,
        COALESCE(SUM(CASE WHEN event_type = 'order_cancelled' THEN total_amount ELSE 0 END), 0) AS total_cancelled_amount
    FROM {{ ref('stg_ecommerce_events') }}
    WHERE event_type IN ('payment_completed', 'order_cancelled')
    GROUP BY event_hour
)

SELECT
    event_hour,
    completed_orders,
    total_gmv,
    ROUND(total_gmv / NULLIF(completed_orders, 0), 0) AS average_order_value,
    cancelled_orders,
    total_cancelled_amount,
    ROUND(cancelled_orders * 100.0 / NULLIF(completed_orders + cancelled_orders, 0), 2) AS cancellation_rate_pct
FROM hourly_transactions
ORDER BY event_hour DESC
