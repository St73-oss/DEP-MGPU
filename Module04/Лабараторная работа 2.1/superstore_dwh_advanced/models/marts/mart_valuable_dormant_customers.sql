-- models/marts/mart_valuable_dormant_customers.sql
-- Индивидуальное задание: "Спящие" ценные клиенты
-- Определяет клиентов из топ-25% по общей выручке, 
-- которые не совершали покупок последние 6 месяцев

WITH customer_rankings AS (
    -- Ранжируем клиентов по общей выручке
    SELECT 
        *,
        PERCENT_RANK() OVER (ORDER BY total_sales DESC) as sales_percentile
    FROM {{ ref('int_orders_pivoted') }}
),

top_25_customers AS (
    -- Выбираем топ-25% клиентов по выручке
    SELECT *
    FROM customer_rankings
    WHERE sales_percentile <= 0.25
),

dormant_customers AS (
    -- Определяем "спящих" клиентов (не было заказов последние 6 месяцев)
    SELECT *
    FROM top_25_customers
    WHERE last_order_date < CURRENT_DATE - INTERVAL '6 months'
)

SELECT 
    customer_id,
    customer_name,
    segment,
    city,
    state,
    
    -- Метрики клиента
    total_orders,
    total_sales,
    total_profit,
    avg_order_value,
    
    -- Даты
    first_order_date,
    last_order_date,
    
    -- Расчет периода "сна"
    CURRENT_DATE - last_order_date::date as days_since_last_order,
    
    -- Дополнительные метрики
    total_quantity,
    avg_discount,
    
    -- Ранжирование
    sales_percentile,
    
    -- Флаг для удобства фильтрации
    'DORMANT_VALUABLE' as customer_status

FROM dormant_customers
ORDER BY total_sales DESC, days_since_last_order DESC
