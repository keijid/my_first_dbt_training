{{ config(
    materialized='table',
    enabled=true
) }}

SELECT
    o.*,
    COALESCE(s.description, '不明なステータス') as status_description,
    s.is_active as is_status_active
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('ref_order_status') }} s
    ON o.status = s.status_code