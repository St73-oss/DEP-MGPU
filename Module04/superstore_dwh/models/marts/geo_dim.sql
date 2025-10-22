-- Создает географический справочник
SELECT
    100 + row_number() OVER (ORDER BY "postal_code", "city", "state") as geo_id,
    "country",
    "city",
    "state",
    "postal_code"
FROM (
    SELECT DISTINCT "country", "city", "state", "postal_code" FROM {{ ref('stg_orders') }}
) as unique_geo