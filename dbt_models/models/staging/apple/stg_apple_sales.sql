{{
  config(
    materialized = 'incremental',
    incremental_strategy='append'
    )
}}

SELECT 
    sale_id, 
    order_date, 
    quantity, 
    price, 
    product_id, 
    customer_id, 
    order_id, 
    payment_method, 
    status,  
    _airbyte_emitted_at as load_timestamp

FROM
{{ source('data_db_src', 'sales') }}

{% if is_incremental() %}
    WHERE _airbyte_emitted_at > (SELECT max(load_timestamp) FROM {{ this }})
{% endif %}