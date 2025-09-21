{{ config(materialized='table') }}

WITH customer_stats AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.city,
        c.years_as_customer,
        COUNT(DISTINCT s.sale_id) as total_orders,
        SUM(s.quantity) as total_items,
        SUM(s.net_revenue) as total_spent,
        AVG(s.net_revenue) as avg_order_value,
        MIN(s.order_date) as first_order_date,
        MAX(s.order_date) as last_order_date,
        COUNT(DISTINCT s.product_id) as unique_products,
        COUNT(DISTINCT p.product_type) as unique_categories
    FROM {{ ref('dim_customers') }} c
    LEFT JOIN {{ ref('fact_sales') }} s ON c.customer_id = s.customer_id
    LEFT JOIN {{ ref('dim_products') }} p ON s.product_id = p.product_id
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    *,
    CASE 
        WHEN total_spent > 200000 THEN 'VIP'
        WHEN total_spent > 100000 THEN 'Лояльный'
        WHEN total_spent > 50000 THEN 'Активный'
        ELSE 'Новый'
    END as customer_segment,
    CASE 
        WHEN total_orders >= 5 THEN 'Частый покупатель'
        WHEN total_orders >= 2 THEN 'Периодический'
        ELSE 'Однократный'
    END as purchase_frequency
FROM customer_stats
WHERE total_orders > 0
ORDER BY total_spent DESC