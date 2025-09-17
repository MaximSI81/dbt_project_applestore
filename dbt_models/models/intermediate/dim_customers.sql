{{ config(materialized='table') }}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    reg_date,
    DATE_PART('year', CURRENT_DATE) - DATE_PART('year', reg_date) as years_as_customer
FROM {{ ref('stg_apple_customers') }}