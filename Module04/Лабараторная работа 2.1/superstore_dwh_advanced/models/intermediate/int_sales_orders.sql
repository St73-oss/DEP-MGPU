-- models/intermediate/int_sales_orders.sql
-- Эта модель объединяет факты со всеми измерениями, создавая
-- широкую, денормализованную таблицу для легкого использования в витринах.

SELECT
    -- Ключи
    f.order_id,
    
    -- Измерения из customer_dim
    c.customer_id,
    c.customer_name,
    
    -- Измерения из product_dim
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.segment,
    
    -- Измерения из geo_dim
    g.city,
    g.state,
    
    -- Измерения из shipping_dim
    s.ship_mode,
    
    -- Даты из calendar_dim (с правильными псевдонимами)
    cal_order.date as order_date,
    cal_ship.date as ship_date,
    
    -- Метрики из sales_fact
    f.sales,
    f.profit,
    f.quantity,
    f.discount
    
FROM {{ source('dw_test', 'sales_fact') }} AS f
LEFT JOIN {{ source('dw_test', 'customer_dim') }} AS c ON f.cust_id = c.cust_id
LEFT JOIN {{ source('dw_test', 'product_dim') }} AS p ON f.prod_id = p.prod_id
LEFT JOIN {{ source('dw_test', 'shipping_dim') }} AS s ON f.ship_id = s.ship_id
LEFT JOIN {{ source('dw_test', 'geo_dim') }} AS g ON f.geo_id = g.geo_id
-- ИСПРАВЛЕНО: Добавляем псевдонимы, так как календарь используется дважды
LEFT JOIN {{ source('dw_test', 'calendar_dim') }} AS cal_order ON f.order_date_id = cal_order.dateid
LEFT JOIN {{ source('dw_test', 'calendar_dim') }} AS cal_ship ON f.ship_date_id = cal_ship.dateid
