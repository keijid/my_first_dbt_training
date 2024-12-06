{{ config(materialized='table') }}

SELECT status_code, description, is_active
FROM UNNEST([
    STRUCT('pending' as status_code, '受付中' as description, TRUE as is_active),
    STRUCT('processing' as status_code, '処理中' as description, TRUE as is_active),
    STRUCT('shipped' as status_code, '発送済' as description, TRUE as is_active),
    STRUCT('delivered' as status_code, '配送完了' as description, TRUE as is_active),
    STRUCT('cancelled' as status_code, 'キャンセル' as description, FALSE as is_active)
])
