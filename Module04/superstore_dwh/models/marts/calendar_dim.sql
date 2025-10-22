-- Создает таблицу-календарь
SELECT
    to_char(date,'yyyymmdd')::int as dateid,
    extract('year' from date)::int as year,
    extract('quarter' from date)::int as quarter,
    extract('month' from date)::int as month,
    extract('week' from date)::int as week,
    date::date,
    to_char(date, 'dy') as week_day,
    -- Проверка на високосный год
    CASE
        WHEN extract('day' from (date + interval '2 month - 1 day')) = 29 THEN 'leap'
        ELSE 'not leap'
    END as leap
FROM generate_series(
    date '2010-01-01',
    date '2030-12-31',
    interval '1 day'
) AS t(date)