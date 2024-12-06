{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    payment_method,
    delivery_date
FROM {{ source('dbt_training', 'orders') }}