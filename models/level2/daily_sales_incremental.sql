{{
    config(
        materialized='incremental',
        unique_key='date_day',
        enabled=true
    )
}}

WITH daily_sales AS (
    SELECT
        DATE(order_date) as date_day,
        COUNT(DISTINCT order_id) as orders_count,
        COUNT(DISTINCT customer_id) as customers_count,
        SUM(total_amount) as total_sales,
        AVG(total_amount) as avg_order_value
    FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
        -- このモデルがすでに存在する場合、既存データの最新日付以降のデータのみを処理
        WHERE DATE(order_date) > (SELECT MAX(date_day) FROM {{ this }})
    {% endif %}
    GROUP BY 1
)
SELECT
    date_day,
    orders_count,
    customers_count,
    total_sales,
    avg_order_value,
    -- メタデータを追加
    CURRENT_TIMESTAMP() as updated_at
FROM daily_sales