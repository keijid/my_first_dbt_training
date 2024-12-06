{{ config(
    materialized='table',
    enabled=true
) }}

WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.country,
        -- 注文関連の集計
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(o.total_amount) as total_spent,
        -- 初回注文と最終注文
        MIN(DATE(o.order_date)) as first_order_date,
        MAX(DATE(o.order_date)) as last_order_date,
        -- 商品購入数
        COUNT(DISTINCT oi.product_id) as unique_products_purchased,
        SUM(oi.quantity) as total_items_purchased
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_orders') }} o
        ON c.customer_id = o.customer_id
    LEFT JOIN {{ source('dbt_training', 'order_items') }} oi
        ON o.order_id = oi.order_id
    GROUP BY 1, 2, 3
    HAVING COUNT(DISTINCT o.order_id) > 0  -- 注文数0のレコードを除外
),
customer_metrics AS (
    SELECT
        *,
        -- 顧客生涯価値（LTV）関連の計算
        ROUND(total_spent / NULLIF(total_orders, 0), 2) as avg_order_value,
        DATE_DIFF(last_order_date, first_order_date, DAY) as customer_lifetime_days,
        -- 購入頻度の計算
        ROUND(total_orders / NULLIF(DATE_DIFF(last_order_date, first_order_date, DAY), 0) * 30, 2) 
            as monthly_purchase_frequency
    FROM customer_orders
)
SELECT
    *,
    -- RFM分析用の指標
    CASE 
        WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) <= 30 THEN 'Active'
        WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) <= 90 THEN 'Recent'
        WHEN DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) <= 180 THEN 'Inactive'
        ELSE 'Lost'
    END as customer_status,
    -- 顧客セグメント
    CASE
        WHEN total_spent >= 1000 AND total_orders >= 5 THEN 'VIP'
        WHEN total_spent >= 500 OR total_orders >= 3 THEN 'Regular'
        ELSE 'New'
    END as customer_segment
FROM customer_metrics