-- models/marts/mart_avg_shipping_duration.sql
SELECT
    s.ship_mode,
    AVG(
        TO_DATE(f.ship_date_id::text, 'YYYYMMDD') 
        - 
        TO_DATE(f.order_date_id::text, 'YYYYMMDD')
    )::NUMERIC(10,2) AS avg_shipping_days
FROM
    {{ ref('sales_fact') }} AS f
JOIN
    {{ ref('shipping_dim') }} AS s
    ON f.ship_id = s.ship_id
GROUP BY
    s.ship_mode
ORDER BY
    avg_shipping_days DESC