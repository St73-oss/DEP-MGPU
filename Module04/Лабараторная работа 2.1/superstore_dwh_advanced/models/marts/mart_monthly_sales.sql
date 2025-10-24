-- models/marts/mart_monthly_sales.sql
-- Эта витрина агрегирует данные по месяцам, категориям и сегментам
-- для использования в BI-дашбордах отдела продаж.

SELECT
    date_trunc('month', order_date)::date as sales_month,
    category,
    segment,
    
    -- Агрегированные метрики
    SUM(sales) as total_sales,
    SUM(profit) as total_profit,
    -- Безопасный расчет маржинальности, чтобы избежать деления на ноль
    COALESCE(SUM(profit) / NULLIF(SUM(sales), 0), 0) as profit_margin,
    COUNT(DISTINCT order_id) as number_of_orders
FROM
    {{ ref('int_sales_orders') }}
GROUP BY 1, 2, 3
ORDER BY sales_month, total_sales DESC
