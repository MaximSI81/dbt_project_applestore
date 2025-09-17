{{
  config(
    materialized = 'incremental',
    incremental_strategy='append'
    )
}}

SELECT
    product_type, 
    color, 
    product_id, 
    model, 
    "storage", 
    current_price_rub, 
    product_name, 
    _airbyte_emitted_at as load_timestamp
  
FROM
{{ source('data_db_src', 'products') }}
{% if is_incremental() %}
    WHERE _airbyte_emitted_at > (SELECT max(load_timestamp) FROM {{ this }})
{% endif %}