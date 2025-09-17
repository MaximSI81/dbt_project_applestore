{{ config(materialized='table') }}

SELECT
    product_id,
    product_name,
    product_type,
    model,
    storage,
    color,
    current_price_rub,
    CASE 
        WHEN product_type = 'iPhone' THEN 'Мобильные устройства'
        WHEN product_type = 'MacBook' THEN 'Компьютеры'
        WHEN product_type = 'iPad' THEN 'Планшеты'
        WHEN product_type = 'Apple Watch' THEN 'Аксессуары'
        WHEN product_type = 'AirPods' THEN 'Аксессуары'
        ELSE 'Другое'
    END as product_category,
    CASE 
        WHEN current_price_rub < 50000 THEN 'Бюджетный'
        WHEN current_price_rub BETWEEN 50000 AND 100000 THEN 'Средний'
        ELSE 'Премиум'
    END as price_segment
FROM {{ ref('stg_apple_products') }}