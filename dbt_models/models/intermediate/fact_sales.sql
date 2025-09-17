{{ config(materialized='table') }}

WITH sales AS (
    SELECT
        s.sale_id,
        s.order_id,
        s.order_date,
        s.customer_id,
        s.product_id,
        s.quantity,
        s.price,
        s.status,
        s.payment_method,
        p.current_price_rub,
        s.quantity * s.price as total_amount,
        CASE WHEN s.status = 'cancelled' THEN 1 ELSE 0 END as is_cancelled
    FROM {{ ref('stg_apple_sales') }} s
    LEFT JOIN {{ ref('stg_apple_products') }} p ON s.product_id = p.product_id
)
SELECT
    s.sale_id,
    s.order_id,
    s.order_date,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.price,
    s.total_amount,
    s.status,
    s.payment_method,
    s.is_cancelled,
    CASE 
        WHEN s.status = 'completed' THEN s.total_amount 
        ELSE 0 
    END as net_revenue
FROM sales s