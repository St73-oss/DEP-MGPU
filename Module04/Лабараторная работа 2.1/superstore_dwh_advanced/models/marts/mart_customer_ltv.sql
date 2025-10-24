-- models/marts/mart_customer_ltv.sql
-- Эта витрина рассчитывает ключевые LTV-метрики по каждому клиенту
-- для анализа ценности и сегментации клиентской базы.

-- LTV-метрики по каждому клиенту
SELECT
    customer_id,
    MIN(customer_name) as customer_name, -- Берем первое по алфавиту имя
    MIN(segment) as segment,             -- Берем первый по алфавиту сегмент
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    COUNT(DISTINCT order_id) as number_of_orders,
    SUM(sales) as total_sales_lifetime,
    AVG(sales) as average_order_value
FROM {{ ref('int_sales_orders') }}
GROUP BY 1 -- Группируем СТРОГО по customer_id
