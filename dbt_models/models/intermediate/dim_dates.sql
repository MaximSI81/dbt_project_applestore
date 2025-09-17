{{ config(materialized='table') }}

WITH date_series AS (
    SELECT 
        generate_series(
            '2023-01-01'::date,
            '2024-12-31'::date,
            '1 day'::interval
        ) as date
)
SELECT
    date as date_id,
    DATE_PART('year', date) as year,
    DATE_PART('quarter', date) as quarter,
    DATE_PART('month', date) as month,
    DATE_PART('week', date) as week,
    DATE_PART('dow', date) as day_of_week,
    TO_CHAR(date, 'Month') as month_name,
    TO_CHAR(date, 'Day') as day_name,
    CASE WHEN DATE_PART('dow', date) IN (0, 6) THEN true ELSE false END as is_weekend
FROM date_series