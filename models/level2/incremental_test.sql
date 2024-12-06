{{
    config(
        materialized='incremental',
        unique_key='date_hour'
    )
}}

-- デバッグ用のコメントを追加
{% if is_incremental() %}
    -- インクリメンタル実行時のみ表示
    {{ log("This is an incremental run!", info=True) }}
{% else %}
    -- 初回実行またはフルリフレッシュ時に表示
    {{ log("This is a full refresh!", info=True) }}
{% endif %}

SELECT
    DATE_TRUNC(order_date, HOUR) as date_hour,
    COUNT(*) as orders_count
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
    -- インクリメンタル実行時のみ実行される条件
    WHERE DATE_TRUNC(order_date, HOUR) > (SELECT MAX(date_hour) FROM {{ this }})
{% endif %}
GROUP BY 1