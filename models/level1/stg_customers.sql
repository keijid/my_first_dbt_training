{{ config(materialized='view') }}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    registration_date,
    country,
    -- 基本的な変換の例
    CONCAT(first_name, ' ', last_name) as full_name,
    EXTRACT(DATE FROM registration_date) as registration_date_only
FROM {{ source('dbt_training', 'customers') }}