{{ config(materialized='table') }}

WITH session_cart_summary AS (
    SELECT
        session_id,
        user_id,
        MIN(CASE WHEN event_type = 'add_to_cart' THEN event_timestamp END) AS first_cart_at,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN event_timestamp END) AS last_cart_at,
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS cart_add_count,
        MAX(CASE WHEN event_type = 'order_created' THEN 1 ELSE 0 END) AS has_order,
        MAX(CASE WHEN event_type = 'payment_completed' THEN 1 ELSE 0 END) AS has_payment
    FROM {{ ref('stg_ecommerce_events') }}
    GROUP BY session_id, user_id
)

SELECT
    session_id,
    user_id,
    first_cart_at,
    last_cart_at,
    cart_add_count,
    has_order,
    has_payment,
    CASE 
        WHEN cart_add_count > 0 AND has_payment = 0 THEN TRUE 
        ELSE FALSE 
    END AS is_cart_abandoned
FROM session_cart_summary
WHERE cart_add_count > 0
