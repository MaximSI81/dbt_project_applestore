{{ config(materialized='table') }}

SELECT
    DATE_TRUNC('month', s.order_date) as sales_month,
    p.product_type,
    c.city,
    s.payment_method,
    COUNT(s.sale_id) as total_orders,
    SUM(s.quantity) as total_quantity,
    SUM(s.net_revenue) as total_revenue,
    AVG(s.net_revenue) as avg_order_value,
    SUM(CASE WHEN s.is_cancelled = 1 THEN 1 ELSE 0 END) as cancelled_orders
FROM {{ ref('fact_sales') }} s
LEFT JOIN {{ ref('dim_products') }} p ON s.product_id = p.product_id
LEFT JOIN {{ ref('dim_customers') }} c ON s.customer_id = c.customer_id
GROUP BY 1, 2, 3, 4
ORDER BY sales_month DESC, total_revenue DESC