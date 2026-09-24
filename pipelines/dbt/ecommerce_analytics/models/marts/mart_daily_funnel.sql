{{ config(materialized='table') }}

WITH base_events AS (
    SELECT * FROM {{ ref('stg_ecommerce_events') }}
),

daily_counts AS (
    SELECT
        event_date,
        COUNT(DISTINCT user_id) AS unique_visitors,
        COUNT(DISTINCT session_id) AS total_sessions,
        COUNT(CASE WHEN event_type = 'item_view' THEN 1 END) AS view_count,
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS cart_count,
        COUNT(CASE WHEN event_type = 'order_created' THEN 1 END) AS order_count,
        COUNT(CASE WHEN event_type = 'payment_completed' THEN 1 END) AS payment_count,
        COUNT(CASE WHEN event_type = 'order_cancelled' THEN 1 END) AS cancel_count
    FROM base_events
    GROUP BY event_date
)

SELECT
    event_date,
    unique_visitors,
    total_sessions,
    view_count,
    cart_count,
    order_count,
    payment_count,
    cancel_count,
    ROUND(cart_count * 100.0 / NULLIF(view_count, 0), 2) AS view_to_cart_rate_pct,
    ROUND(order_count * 100.0 / NULLIF(cart_count, 0), 2) AS cart_to_order_rate_pct,
    ROUND(payment_count * 100.0 / NULLIF(order_count, 0), 2) AS order_to_payment_rate_pct,
    ROUND(payment_count * 100.0 / NULLIF(view_count, 0), 2) AS overall_conversion_rate_pct
FROM daily_counts
ORDER BY event_date DESC
