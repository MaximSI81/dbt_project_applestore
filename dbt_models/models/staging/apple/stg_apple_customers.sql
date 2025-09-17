{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    city,
    reg_date,
    -- Используем метаданные Airbyte для инкрементальной загрузки
    _airbyte_emitted_at as load_timestamp
    
FROM {{ source('data_db_src', 'customers') }}

{% if is_incremental() %}
-- Используем метку времени Airbyte для инкрементальной загрузки
WHERE _airbyte_emitted_at > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}