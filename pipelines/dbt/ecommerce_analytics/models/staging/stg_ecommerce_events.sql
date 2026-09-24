{{ config(materialized='view') }}

WITH raw_iceberg AS (
    SELECT *
    FROM iceberg_scan('warehouse/ecommerce/silver_events')
)

SELECT
    event_id,
    event_type,
    event_timestamp,
    CAST(event_timestamp AS DATE) AS event_date,
    DATE_TRUNC('hour', event_timestamp) AS event_hour,
    user_id,
    session_id,
    sequence_number,
    correlation_id,
    traceparent,
    product_id,
    order_id,
    items,
    total_amount,
    payment_method,
    cancellation_reason,
    broker_timestamp,
    kafka_partition,
    kafka_offset
FROM raw_iceberg
