{{ config(materialized='table') }}

SELECT
    p.product_type,
    p.product_category,
    p.model,
    p.price_segment,
    DATE_TRUNC('month', s.order_date) as sales_month,
    COUNT(s.sale_id) as total_orders,
    SUM(s.quantity) as total_quantity,
    SUM(s.net_revenue) as total_revenue,
    AVG(s.price) as avg_price,
    SUM(CASE WHEN s.is_cancelled = 1 THEN 1 ELSE 0 END) as cancelled_orders,
    ROUND(SUM(CASE WHEN s.is_cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(s.sale_id), 2) as cancellation_rate
FROM {{ ref('fact_sales') }} s
LEFT JOIN {{ ref('dim_products') }} p ON s.product_id = p.product_id
GROUP BY 1, 2, 3, 4, 5
ORDER BY sales_month DESC, total_revenue DESC