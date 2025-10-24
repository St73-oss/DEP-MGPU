-- models/intermediate/int_orders_pivoted.sql
-- Промежуточная модель для индивидуального задания
-- Создает сводную таблицу заказов с агрегированными метриками по клиентам

-- Сводная таблица заказов с агрегированными метриками по клиентам
SELECT
    customer_id,
    MIN(customer_name) as customer_name, -- <--- ДОБАВЛЕНА ФУНКЦИЯ MIN()
    MIN(segment) as segment,             -- <--- ДОБАВЛЕНА ФУНКЦИЯ MIN()
    MIN(city) as city,                   -- <--- ДОБАВЛЕНА ФУНКЦИЯ MIN()
    MIN(state) as state,                 -- <--- ДОБАВЛЕНА ФУНКЦИЯ MIN()
    COUNT(DISTINCT order_id) as total_orders,
    SUM(sales) as total_sales,
    SUM(profit) as total_profit,
    AVG(sales) as avg_order_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    SUM(quantity) as total_quantity,
    AVG(discount) as avg_discount
FROM {{ ref('int_sales_orders') }}
GROUP BY 1 -- Группируем СТРОГО по customer_id
