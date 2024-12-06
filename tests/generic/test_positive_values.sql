{% test positive_values(model, column_name) %}

WITH validation AS (
    SELECT
        {{ column_name }} as value
    FROM {{ model }}
    WHERE {{ column_name }} <= 0
)

SELECT *
FROM validation
WHERE value IS NOT NULL

{% endtest %}