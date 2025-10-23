-- =============================================
-- Проверка качества данных — Superstore
-- =============================================

--1. Проверка созданной таблицы доставки (dim)
select * from dw.shipping_dim sd;

--2. Проверка созданной таблицы клиенты (dim)
select * from dw.customer_dim cd;

--3. Проверка созданной таблицы география (dim)
select * from dw.geo_dim
where city = 'Burlington'

--4. Проверка созданной таблицы продукты (dim)
select * from dw.product_dim cd;

--5. Проверка созданной таблицы календарь(dim)
select * from dw.calendar_dim; 

--6. Проверка созданной таблицы продажи (Fact)
select count(*) from dw.sales_fact sf
inner join dw.shipping_dim s on sf.ship_id=s.ship_id
inner join dw.geo_dim g on sf.geo_id=g.geo_id
inner join dw.product_dim p on sf.prod_id=p.prod_id
inner join dw.customer_dim cd on sf.cust_id=cd.cust_id;