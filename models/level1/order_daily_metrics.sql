{{ config(
    materialized='table',
    enabled=true
) }}

WITH daily_metrics AS (
    SELECT
        -- 日付の集計単位を作成
        DATE(order_date) as order_date,
        -- 基本的な集計指標
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(total_amount) as total_sales,
        -- 平均購入額
        AVG(total_amount) as avg_order_value,
        -- 支払方法の内訳
        COUNTIF(payment_method = 'credit_card') as credit_card_count,
        COUNTIF(payment_method = 'debit_card') as debit_card_count,
        COUNTIF(payment_method = 'bank_transfer') as bank_transfer_count
    FROM {{ ref('stg_orders') }}
    GROUP BY 1
),
daily_metrics_with_growth AS (
    SELECT
        *,
        -- 前日比較（ウィンドウ関数）
        LAG(total_orders) OVER (ORDER BY order_date) as prev_day_orders,
        LAG(total_sales) OVER (ORDER BY order_date) as prev_day_sales,
        -- 7日移動平均
        AVG(total_sales) OVER (
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as sales_7day_moving_avg
    FROM daily_metrics
)
SELECT
    *,
    -- 前日比の計算
    COALESCE(ROUND((total_orders - prev_day_orders) / prev_day_orders * 100, 2), 0) as orders_day_over_day_pct,
    COALESCE(ROUND((total_sales - prev_day_sales) / prev_day_sales * 100, 2), 0) as sales_day_over_day_pct
FROM daily_metrics_with_growth
ORDER BY order_date DESC