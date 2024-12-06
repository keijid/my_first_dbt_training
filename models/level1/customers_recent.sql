{{
    config(
        materialized='table',
        enabled=true
    )
}}

SELECT
    stg.*,
    DATE_DIFF(CURRENT_DATE(), stg.registration_date_only, DAY) as days_since_registration
FROM {{ ref('stg_customers') }} as stg
WHERE DATE_DIFF(CURRENT_DATE(), stg.registration_date_only, DAY) <= 180