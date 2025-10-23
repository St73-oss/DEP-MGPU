-- =============================================
-- DDL-скрипт прототипа базы данных Superstore
-- Слои: stg (staging), Raw Data Layer (промежуточный слой) и dw (data warehouse)
-- =============================================

-- Создание схем
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;

-- =============================================
-- STG слой: исходные таблицы
-- =============================================

-- Таблица заказов (staging)
DROP TABLE IF EXISTS stg.orders;
CREATE TABLE stg.orders (
    Row_ID        INTEGER  NOT NULL PRIMARY KEY,
    Order_ID      VARCHAR(14) NOT NULL,
    Order_Date    DATE NOT NULL,
    Ship_Date     DATE NOT NULL,
    Ship_Mode     VARCHAR(14) NOT NULL,
    Customer_ID   VARCHAR(8) NOT NULL,
    Customer_Name VARCHAR(22) NOT NULL,
    Segment       VARCHAR(11) NOT NULL,
    Country       VARCHAR(13) NOT NULL,
    City          VARCHAR(17) NOT NULL,
    State         VARCHAR(20) NOT NULL,
    Postal_Code   VARCHAR(50),  -- VARCHAR для сохранения ведущих нулей
    Region        VARCHAR(7) NOT NULL,
    Product_ID    VARCHAR(15) NOT NULL,
    Category      VARCHAR(15) NOT NULL,
    SubCategory   VARCHAR(11) NOT NULL,
    Product_Name  VARCHAR(127) NOT NULL,
    Sales         NUMERIC(9,4) NOT NULL,
    Quantity      INTEGER NOT NULL,
    Discount      NUMERIC(4,2) NOT NULL,
    Profit        NUMERIC(21,16) NOT NULL
);

-- =============================================
-- RD слой: промежуточный слой
-- =============================================

-- Таблица Заказов
DROP TABLE IF EXISTS orders;
CREATE TABLE orders(
   Row_ID        INTEGER  NOT NULL PRIMARY KEY 
  ,Order_ID      VARCHAR(14) NOT NULL
  ,Order_Date    DATE  NOT NULL
  ,Ship_Date     DATE  NOT NULL
  ,Ship_Mode     VARCHAR(14) NOT NULL
  ,Customer_ID   VARCHAR(8) NOT NULL
  ,Customer_Name VARCHAR(22) NOT NULL
  ,Segment       VARCHAR(11) NOT NULL
  ,Country       VARCHAR(13) NOT NULL
  ,City          VARCHAR(17) NOT NULL
  ,State         VARCHAR(20) NOT NULL
  ,Postal_Code   INTEGER 
  ,Region        VARCHAR(7) NOT NULL
  ,Product_ID    VARCHAR(15) NOT NULL
  ,Category      VARCHAR(15) NOT NULL
  ,SubCategory   VARCHAR(11) NOT NULL
  ,Product_Name  VARCHAR(127) NOT NULL
  ,Sales         NUMERIC(9,4) NOT NULL
  ,Quantity      INTEGER  NOT NULL
  ,Discount      NUMERIC(4,2) NOT NULL
  ,Profit        NUMERIC(21,16) NOT NULL
);


-- Таблица возвратов
DROP TABLE IF EXISTS returns;
CREATE TABLE returns (
    Returned   VARCHAR(17) NOT NULL,
    Order_ID   VARCHAR(20) NOT NULL
);

-- Таблица менеджеров
DROP TABLE IF EXISTS people;
CREATE TABLE people (
    Person VARCHAR(17) NOT NULL PRIMARY KEY,
    Region VARCHAR(7) NOT NULL
);

-- =============================================
-- DW слой: измерения и факты
-- =============================================

-- Измерение: Способы доставки
DROP TABLE IF EXISTS dw.shipping_dim;
CREATE TABLE dw.shipping_dim (
    ship_id       SERIAL NOT NULL,
    shipping_mode VARCHAR(14) NOT NULL,
    CONSTRAINT PK_shipping_dim PRIMARY KEY (ship_id)
);

-- Измерение: Клиенты
DROP TABLE IF EXISTS dw.customer_dim;
CREATE TABLE dw.customer_dim (
    cust_id       SERIAL NOT NULL,
    customer_id   VARCHAR(8) NOT NULL,
    customer_name VARCHAR(22) NOT NULL,
    CONSTRAINT PK_customer_dim PRIMARY KEY (cust_id)
);

-- Измерение: География
DROP TABLE IF EXISTS dw.geo_dim;
CREATE TABLE dw.geo_dim (
    geo_id        SERIAL NOT NULL,
    country       VARCHAR(13) NOT NULL,
    city          VARCHAR(17) NOT NULL,
    state         VARCHAR(20) NOT NULL,
    postal_code   VARCHAR(20) NULL,
    CONSTRAINT PK_geo_dim PRIMARY KEY (geo_id)
);

-- Измерение: Продукты
DROP TABLE IF EXISTS dw.product_dim;
CREATE TABLE dw.product_dim (
    prod_id       SERIAL NOT NULL,
    product_id    VARCHAR(50) NOT NULL,
    product_name  VARCHAR(127) NOT NULL,
    category      VARCHAR(15) NOT NULL,
    sub_category  VARCHAR(11) NOT NULL,
    segment       VARCHAR(11) NOT NULL,
    CONSTRAINT PK_product_dim PRIMARY KEY (prod_id)
);

-- Измерение: Календарь
DROP TABLE IF EXISTS dw.calendar_dim;
CREATE TABLE dw.calendar_dim (
    dateid    SERIAL NOT NULL,
    year      INT NOT NULL,
    quarter   INT NOT NULL,
    month     INT NOT NULL,
    week      INT NOT NULL,
    date      DATE NOT NULL,
    week_day  VARCHAR(20) NOT NULL,
    leap      BOOLEAN NOT NULL,
    CONSTRAINT PK_calendar_dim PRIMARY KEY (dateid)
);

-- Измерение: Менеджеры (для задания 1)
DROP TABLE IF EXISTS dw.people_dim;
CREATE TABLE dw.people_dim (
    person_id   SERIAL NOT NULL,
    person_name VARCHAR(17) NOT NULL,
    region      VARCHAR(7) NOT NULL,
    CONSTRAINT PK_people_dim PRIMARY KEY (person_id)
);

-- Таблица фактов: Продажи
DROP TABLE IF EXISTS dw.sales_fact;
CREATE TABLE dw.sales_fact (
    sales_id        SERIAL NOT NULL,
    cust_id         INTEGER NOT NULL,
    order_date_id   INTEGER NOT NULL,
    ship_date_id    INTEGER NOT NULL,
    prod_id         INTEGER NOT NULL,
    ship_id         INTEGER NOT NULL,
    geo_id          INTEGER NOT NULL,
    order_id        VARCHAR(25) NOT NULL,
    sales           NUMERIC(9,4) NOT NULL,
    profit          NUMERIC(21,16) NOT NULL,
    quantity        INT4 NOT NULL,
    discount        NUMERIC(4,2) NOT NULL,
    CONSTRAINT PK_sales_fact PRIMARY KEY (sales_id)
);