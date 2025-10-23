-- Задание 1: Выручка по менеджерам
SELECT 
    p.Person AS manager,
    SUM(o.Sales) AS total_revenue
FROM stg.orders o
JOIN stg.people p ON o.Region = p.Region
GROUP BY p.Person
ORDER BY total_revenue DESC;

-- Задание 2: Таблица времени доставки
DROP TABLE IF EXISTS dw.delivery_time;
CREATE TABLE dw.delivery_time AS
SELECT
    o.order_id,
    o.row_id,
    o.order_date,
    o.ship_date,
    (o.ship_date - o.order_date) AS delivery_days,
    s.shipping_mode,
    g.city,
    g.state,
    g.country
FROM stg.orders o
JOIN dw.shipping_dim s ON o.ship_mode = s.shipping_mode
JOIN dw.geo_dim g 
    ON o.postal_code = g.postal_code 
    AND o.country = g.country 
    AND o.city = g.city 
    AND o.state = g.state;

-- Задание 3: Топ-10 продуктов по сумме скидок (в деньгах)
SELECT 
    p.product_name,
    SUM(s.sales * s.discount) AS total_discount_amount
FROM dw.sales_fact s
JOIN dw.product_dim p ON s.prod_id = p.prod_id
GROUP BY p.product_name
ORDER BY total_discount_amount DESC
LIMIT 10;