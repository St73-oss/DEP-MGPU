-- Создает справочник клиентов
SELECT
    100 + row_number() OVER (ORDER BY "customer_id") as cust_id,
    "customer_id",
    "customer_name"
FROM (
    SELECT DISTINCT "customer_id", "customer_name" FROM {{ ref('stg_orders') }}
) as unique_customers