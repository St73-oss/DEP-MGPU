# Практическая работа 2-2: Создание dbt-проекта для аналитических витрин

## Бизнес-кейс

Вы — аналитический инженер в "Superstore". После успешного внедрения базового DWH (работа 2.1) вы стандартизировали ключевые сущности. Однако отделы продаж, маркетинга и логистики сообщают, что основная таблица фактов (sales_fact) слишком сложна и громоздка для их повседневных задач в BI-инструментах.

**Задача**: развить dbt-проект до полноценного аналитического решения. Построить поверх DWH специализированные, быстрые и удобные аналитические витрины (marts). Каждая витрина должна быть спроектирована для ответа на конкретные бизнес-вопросы.

## Цель работы

- Реализовать многоуровневую архитектуру dbt-проектов: staging -> intermediate -> marts
- Научиться создавать бизнес-ориентированные аналитические витрины поверх основного DWH
- Освоить продвинутые концепции dbt: промежуточные модели, снимки данных и кастомные generic-тесты
- Внедрить элементы управления данными (Data Governance) через документацию и описание конечных потребителей данных

## Архитектура проекта

### Многоуровневая архитектура dbt

```
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL Database                      │
├─────────────────────────────────────────────────────────────────┤
│  stg schema          │  dw_test schema      │  dw_intermediate  │
│  (staging)           │  (marts)             │  schema           │
│                      │                      │                   │
│  ┌─────────────────┐ │  ┌─────────────────┐ │  ┌──────────────┐ │
│  │ sources.yml     │ │  │ mart_monthly_   │ │  │ int_sales_   │ │
│  │ (источники)     │ │  │ sales           │ │  │ orders       │ │
│  └─────────────────┘ │  └─────────────────┘ │  └──────────────┘ │
│                      │  ┌─────────────────┐ │  ┌──────────────┐ │
│                      │  │ mart_customer_  │ │  │ int_orders_  │ │
│                      │  │ ltv             │ │  │ pivoted      │ │
│                      │  └─────────────────┘ │  └──────────────┘ │
│                      │  ┌─────────────────┐ │                   │
│                      │  │ mart_valuable_  │ │                   │
│                      │  │ dormant_customers│ │                   │
│                      │  └─────────────────┘ │                   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        dbt Project                             │
├─────────────────────────────────────────────────────────────────┤
│  models/staging/     │  models/intermediate/ │  models/marts/   │
│                      │                       │                  │
│  ┌─────────────────┐ │  ┌─────────────────┐  │  ┌─────────────┐ │
│  │ sources.yml     │ │  │ int_sales_      │  │  │ mart_monthly│ │
│  │                 │ │  │ orders.sql      │  │  │ _sales.sql  │ │
│  └─────────────────┘ │  └─────────────────┘  │  └─────────────┘ │
│                      │  ┌─────────────────┐  │  ┌─────────────┐ │
│                      │  │ int_orders_     │  │  │ mart_customer│ │
│                      │  │ pivoted.sql     │  │  │ _ltv.sql    │ │
│                      │  └─────────────────┘  │  └─────────────┘ │
│                      │                       │  ┌─────────────┐ │
│                      │                       │  │ mart_valuable│ │
│                      │                       │  │ _dormant_   │ │
│                      │                       │  │ customers.sql│ │
│                      │                       │  └─────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Data Flow (staging → intermediate → marts)   │
├─────────────────────────────────────────────────────────────────┤
│  sources.yml         │  int_sales_orders    │  mart_monthly_sales│
│  (источники из       │  (денормализованная  │  (агрегация по     │
│   dw_test schema)    │   таблица фактов)    │   месяцам)         │
│                      │                      │                   │
│  ┌─────────────────┐ │  ┌─────────────────┐ │  ┌─────────────┐ │
│  │ sales_fact      │ │  │ JOIN всех       │ │  │ GROUP BY    │ │
│  │ customer_dim    │ │  │ измерений       │ │  │ month,      │ │
│  │ product_dim     │ │  │                 │ │  │ category    │ │
│  │ shipping_dim    │ │  │                 │ │  │ segment     │ │
│  │ geo_dim         │ │  │                 │ │  │             │ │
│  │ calendar_dim    │ │  │                 │ │  │             │ │
│  └─────────────────┘ │  └─────────────────┘ │  └─────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Поток данных

1. **Staging** → Определение источников из схемы `dw_test`
2. **Intermediate** → Денормализация и подготовка данных
3. **Marts** → Бизнес-витрины для аналитики

## Структура проекта

```
superstore_dwh_advanced/
├── analyses/             # Аналитические запросы
├── logs/                 # Логи dbt
├── macros/               # Макросы dbt
├── models/               # Модели dbt
│   ├── staging/          # Staging модели
│   │   └── sources.yml   # Определение источников данных
│   ├── intermediate/     # Промежуточные модели
│   │   ├── int_sales_orders.sql      # Денормализованная таблица фактов
│   │   └── int_orders_pivoted.sql    # Сводная таблица по клиентам
│   └── marts/            # Аналитические витрины
│       ├── mart_monthly_sales.sql           # Витрина месячных продаж
│       ├── mart_customer_ltv.sql            # Витрина LTV клиентов
│       ├── mart_valuable_dormant_customers.sql  # Спящие ценные клиенты
│       ├── schema.yml    # Схема и тесты для витрин
│       └── exposures.yml # Потребители данных (дашборды, отчеты)
├── seeds/                # Семена данных
├── snapshots/            # Снимки данных
│   └── snapshot_product_dim.sql  # Снимок изменений продуктов
├── tests/                # Тесты
│   └── generic/          # Generic тесты
│       └── test_is_positive.sql   # Кастомный тест на положительные значения
├── target/               # Скомпилированные файлы (создается автоматически)
├── dbt_project.yml       # Конфигурация проекта
├── profiles.yml          # Настройки подключения к БД
└── README.md             # Документация проекта
```

## Что должно быть в PostgreSQL

### Предварительные требования

Перед запуском проекта убедитесь, что в PostgreSQL есть:

#### 1. **База данных `superstore`**
```sql
CREATE DATABASE superstore;
```

#### 2. **Схемы**
```sql
-- Схема для staging моделей
CREATE SCHEMA IF NOT EXISTS stg;

-- Схема для промежуточных моделей
CREATE SCHEMA IF NOT EXISTS dw_intermediate;

-- Схема для витрин (marts)
CREATE SCHEMA IF NOT EXISTS dw_test;

-- Схема для снимков данных
CREATE SCHEMA IF NOT EXISTS dw_snapshots;
```

#### 3. **Исходные данные в схеме `dw_test`**

**Таблица фактов:**
```sql
-- sales_fact
CREATE TABLE dw_test.sales_fact (
    order_id VARCHAR,
    cust_id INTEGER,
    prod_id INTEGER,
    ship_id INTEGER,
    geo_id INTEGER,
    order_date_id INTEGER,
    ship_date_id INTEGER,
    sales DECIMAL,
    profit DECIMAL,
    quantity INTEGER,
    discount DECIMAL
);
```

**Таблицы измерений:**
```sql
-- customer_dim
CREATE TABLE dw_test.customer_dim (
    cust_id INTEGER PRIMARY KEY,
    customer_id VARCHAR,
    customer_name VARCHAR
);

-- product_dim
CREATE TABLE dw_test.product_dim (
    prod_id INTEGER PRIMARY KEY,
    product_id VARCHAR,
    product_name VARCHAR,
    category VARCHAR,
    sub_category VARCHAR,
    segment VARCHAR
);

-- shipping_dim
CREATE TABLE dw_test.shipping_dim (
    ship_id INTEGER PRIMARY KEY,
    ship_mode VARCHAR
);

-- geo_dim
CREATE TABLE dw_test.geo_dim (
    geo_id INTEGER PRIMARY KEY,
    city VARCHAR,
    state VARCHAR
);

-- calendar_dim
CREATE TABLE dw_test.calendar_dim (
    dateid INTEGER PRIMARY KEY,
    date DATE
);
```

#### 4. **Проверка наличия данных**

```sql
-- Проверьте, что таблицы существуют и содержат данные
SELECT COUNT(*) FROM dw_test.sales_fact;
SELECT COUNT(*) FROM dw_test.customer_dim;
SELECT COUNT(*) FROM dw_test.product_dim;
SELECT COUNT(*) FROM dw_test.shipping_dim;
SELECT COUNT(*) FROM dw_test.geo_dim;
SELECT COUNT(*) FROM dw_test.calendar_dim;
```

### Результат выполнения проекта

После успешного запуска `dbt run` в PostgreSQL будут созданы:

#### **Схема `dw_intermediate`:**
- `int_sales_orders` - денормализованная таблица фактов
- `int_orders_pivoted` - сводная таблица по клиентам

#### **Схема `dw_test`:**
- `mart_monthly_sales` - витрина месячных продаж
- `mart_customer_ltv` - витрина LTV клиентов  
- `mart_valuable_dormant_customers` - спящие ценные клиенты

#### **Схема `dw_snapshots`:**
- `snapshot_product_dim` - снимок изменений продуктов

### Проверка результатов

```sql
-- Проверьте созданные таблицы
\dt dw_intermediate.*
\dt dw_test.*
\dt dw_snapshots.*

-- Проверьте данные
SELECT COUNT(*) FROM dw_intermediate.int_sales_orders;
SELECT COUNT(*) FROM dw_test.mart_monthly_sales;
SELECT COUNT(*) FROM dw_test.mart_customer_ltv;
SELECT COUNT(*) FROM dw_test.mart_valuable_dormant_customers;
```

## Пошаговая инструкция по созданию проекта

### 1. Создание структуры каталогов

```bash
# Создайте новый каталог для проекта
mkdir superstore_dwh_advanced
cd superstore_dwh_advanced

# Создайте необходимые папки
mkdir models models\staging models\intermediate models\marts
mkdir tests tests\generic
mkdir snapshots macros seeds analyses logs target
```

### 2. Настройка конфигурации dbt

#### dbt_project.yml
```yaml
name: 'superstore_dwh_advanced'
version: '1.0.0'
config-version: 2

profile: 'superstore_dwh_advanced'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  superstore_dwh_advanced:
    staging:
      +materialized: view
      +schema: stg
    intermediate:
      +materialized: view
      +schema: dw_intermediate
    marts:
      +materialized: table
      +schema: dw_test
```

#### profiles.yml
```yaml
superstore_dwh_advanced:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: postgres
      port: 5432
      dbname: superstore
      schema: public
      threads: 4
      keepalives_idle: 0
```

### 3. Создание промежуточных моделей

#### models/intermediate/int_sales_orders.sql
```sql
-- Объединяет факты со всеми измерениями
SELECT
    f.order_id,
    c.customer_id,
    c.customer_name,
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.segment,
    g.city,
    g.state,
    s.ship_mode,
    cal_order.date as order_date,
    cal_ship.date as ship_date,
    f.sales,
    f.profit,
    f.quantity,
    f.discount
FROM {{ ref('sales_fact') }} AS f
LEFT JOIN {{ ref('customer_dim') }} AS c ON f.cust_id = c.cust_id
LEFT JOIN {{ ref('product_dim') }} AS p ON f.prod_id = p.prod_id
LEFT JOIN {{ ref('shipping_dim') }} AS s ON f.ship_id = s.ship_id
LEFT JOIN {{ ref('geo_dim') }} AS g ON f.geo_id = g.geo_id
LEFT JOIN {{ ref('calendar_dim') }} AS cal_order ON f.order_date_id = cal_order.dateid
LEFT JOIN {{ ref('calendar_dim') }} AS cal_ship ON f.ship_date_id = cal_ship.dateid
```

#### models/intermediate/int_orders_pivoted.sql
```sql
-- Сводная таблица заказов с агрегированными метриками по клиентам
SELECT
    customer_id,
    customer_name,
    segment,
    city,
    state,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(sales) as total_sales,
    SUM(profit) as total_profit,
    AVG(sales) as avg_order_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    SUM(quantity) as total_quantity,
    AVG(discount) as avg_discount
FROM {{ ref('int_sales_orders') }}
GROUP BY 1, 2, 3, 4, 5
```

### 4. Создание аналитических витрин

#### models/marts/mart_monthly_sales.sql
```sql
-- Агрегированные данные по продажам по месяцам
SELECT
    date_trunc('month', order_date)::date as sales_month,
    category,
    segment,
    SUM(sales) as total_sales,
    SUM(profit) as total_profit,
    COALESCE(SUM(profit) / NULLIF(SUM(sales), 0), 0) as profit_margin,
    COUNT(DISTINCT order_id) as number_of_orders
FROM {{ ref('int_sales_orders') }}
GROUP BY 1, 2, 3
ORDER BY sales_month, total_sales DESC
```

#### models/marts/mart_customer_ltv.sql
```sql
-- LTV-метрики по каждому клиенту
SELECT
    customer_id,
    customer_name,
    segment,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    COUNT(DISTINCT order_id) as number_of_orders,
    SUM(sales) as total_sales_lifetime,
    AVG(sales) as average_order_value
FROM {{ ref('int_sales_orders') }}
GROUP BY 1, 2, 3
```

#### models/marts/mart_valuable_dormant_customers.sql
```sql
-- Спящие ценные клиенты (индивидуальное задание)
WITH customer_rankings AS (
    SELECT 
        *,
        PERCENT_RANK() OVER (ORDER BY total_sales DESC) as sales_percentile
    FROM {{ ref('int_orders_pivoted') }}
),
top_25_customers AS (
    SELECT *
    FROM customer_rankings
    WHERE sales_percentile <= 0.25
),
dormant_customers AS (
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
    total_orders,
    total_sales,
    total_profit,
    avg_order_value,
    first_order_date,
    last_order_date,
    CURRENT_DATE - last_order_date::date as days_since_last_order,
    total_quantity,
    avg_discount,
    sales_percentile,
    'DORMANT_VALUABLE' as customer_status
FROM dormant_customers
ORDER BY total_sales DESC, days_since_last_order DESC
```

### 5. Создание кастомных тестов

#### tests/generic/test_is_positive.sql
```sql
{% test is_positive(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} < 0
{% endtest %}
```

### 6. Настройка тестов в schema.yml

```yaml
version: 2
models:
  - name: mart_monthly_sales
    columns:
      - name: total_sales
        tests:
          - not_null
          - is_positive
      - name: number_of_orders
        tests:
          - is_positive
  # ... остальные модели
```

### 7. Создание снимков данных

#### snapshots/snapshot_product_dim.sql
```sql
{% snapshot snapshot_product_dim %}
{{
    config(
      target_schema='dw_snapshots',
      strategy='check',
      unique_key='prod_id',
      check_cols=['segment', 'category'],
    )
}}
SELECT prod_id, product_id, segment, category FROM {{ ref('product_dim') }}
{% endsnapshot %}
```

### 8. Декларация потребителей данных

#### models/marts/exposures.yml
```yaml
version: 2
exposures:
  - name: executive_dashboard
    type: dashboard
    maturity: high
    owner:
      name: "Sales Department"
      email: "sales@superstore.com"
    depends_on:
      - ref('mart_monthly_sales')
      - ref('mart_customer_ltv')
```

## Команды для запуска

### 1. Активация виртуального окружения

```bash
# Активируйте виртуальное окружение dbt-env
source dbt-env/bin/activate

# Или если используете conda
conda activate dbt-env
```

### 2. Переход в каталог проекта

```bash
cd superstore_dwh_advanced/
```

### 3. Проверка подключения к базе данных

```bash
# Проверьте подключение к PostgreSQL
dbt debug
```

### 4. Запуск моделей

```bash
# Запуск всех моделей
dbt run

# Запуск конкретной модели
dbt run --select int_sales_orders

# Запуск промежуточных моделей
dbt run --select intermediate

# Запуск витрин
dbt run --select marts

# Запуск конкретной витрины
dbt run --select mart_monthly_sales
```

### 5. Запуск тестов

```bash
# Запуск всех тестов
dbt test

# Запуск тестов для конкретной модели
dbt test --select mart_monthly_sales

# Запуск кастомных тестов
dbt test --select test_type:generic
```

### 6. Создание снимков данных

```bash
# Создание снимков
dbt snapshot

# Запуск снимков с обновлением
dbt snapshot --select snapshot_product_dim
```

### 7. Генерация и просмотр документации

```bash
# Генерация документации
dbt docs generate

# Запуск локального сервера документации
dbt docs serve

# Откройте браузер и перейдите на http://localhost:8080
```

### 8. Проверка результатов в базе данных

```bash
# Подключение к PostgreSQL
psql -h localhost -U postgres -d superstore

# Проверка созданных схем
\dn

# Проверка промежуточных моделей
\dt dw_intermediate.*

# Проверка витрин
\dt dw_test.*

# Проверка снимков
\dt dw_snapshots.*

# Примеры запросов для проверки данных
SELECT COUNT(*) FROM dw_intermediate.int_sales_orders;
SELECT COUNT(*) FROM dw_test.mart_monthly_sales;
SELECT COUNT(*) FROM dw_test.mart_customer_ltv;
SELECT COUNT(*) FROM dw_test.mart_valuable_dormant_customers;
```

## Результаты

После выполнения всех шагов вы получите:

1. **Промежуточные модели** в схеме `dw_intermediate`
2. **Аналитические витрины** в схеме `dw_test`
3. **Кастомные тесты** для проверки качества данных
4. **Снимки данных** для отслеживания истории изменений
5. **Документацию** с описанием потребителей данных

## Проверка результатов

### 1. Проверка структуры базы данных

```sql
-- Подключение к PostgreSQL
psql -h localhost -U postgres -d superstore

-- Проверка созданных схем
\dn

-- Должны быть созданы схемы:
-- - stg (staging модели)
-- - dw_intermediate (промежуточные модели)
-- - dw_test (витрины)
-- - dw_snapshots (снимки данных)
```

### 2. Проверка промежуточных моделей

```sql
-- Проверка int_sales_orders
SELECT COUNT(*) as total_records FROM dw_intermediate.int_sales_orders;
SELECT * FROM dw_intermediate.int_sales_orders LIMIT 5;

-- Проверка int_orders_pivoted
SELECT COUNT(*) as total_customers FROM dw_intermediate.int_orders_pivoted;
SELECT * FROM dw_intermediate.int_orders_pivoted LIMIT 5;
```

### 3. Проверка аналитических витрин

```sql
-- Проверка mart_monthly_sales
SELECT COUNT(*) as monthly_records FROM dw_test.mart_monthly_sales;
SELECT * FROM dw_test.mart_monthly_sales ORDER BY sales_month DESC LIMIT 10;

-- Проверка mart_customer_ltv
SELECT COUNT(*) as customer_records FROM dw_test.mart_customer_ltv;
SELECT * FROM dw_test.mart_customer_ltv ORDER BY total_sales_lifetime DESC LIMIT 10;

-- Проверка mart_valuable_dormant_customers
SELECT COUNT(*) as dormant_customers FROM dw_test.mart_valuable_dormant_customers;
SELECT * FROM dw_test.mart_valuable_dormant_customers ORDER BY total_sales DESC LIMIT 10;
```

### 4. Проверка снимков данных

```sql
-- Проверка snapshot_product_dim
SELECT COUNT(*) as snapshot_records FROM dw_snapshots.snapshot_product_dim;
SELECT * FROM dw_snapshots.snapshot_product_dim LIMIT 5;
```

### 5. Проверка качества данных

```bash
# Запуск всех тестов
dbt test

# Проверка результатов тестов
# Все тесты должны пройти успешно
```

### 6. Проверка документации

```bash
# Генерация и запуск документации
dbt docs generate
dbt docs serve

# Откройте браузер: http://localhost:8080
# Проверьте:
# - Модели и их зависимости
# - Exposures (потребители данных)
# - Тесты и их результаты
```

## Устранение неполадок

### Проблема: Ошибка подключения к базе данных

```bash
# Проверьте настройки в profiles.yml
dbt debug

# Убедитесь, что PostgreSQL запущен
sudo systemctl status postgresql

# Проверьте подключение вручную
psql -h localhost -U postgres -d superstore
```

### Проблема: Модели не находят зависимости

```bash
# Проверьте, что все базовые модели из предыдущей работы существуют
# Убедитесь, что схемы stg и dw_test содержат необходимые таблицы

# Запустите модели по порядку
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
```

### Проблема: Тесты не проходят

```bash
# Запустите тесты с подробным выводом
dbt test --verbose

# Проверьте данные вручную
psql -h localhost -U postgres -d superstore -c "SELECT * FROM dw_test.mart_monthly_sales WHERE total_sales < 0;"
```

### Проблема: Ошибки парсинга dbt

```bash
# Очистите кэш dbt
dbt clean

# Удалите папку target
rm -rf target/

# Перезапустите парсинг
dbt parse
```

### Проблема: Ошибки в exposures.yml

```bash
# Проверьте синтаксис YAML
cat models/marts/exposures.yml

# Убедитесь, что все типы корректны (dashboard, notebook, analysis, ml, application)
# Исправьте тип 'report' на 'dashboard'
```

### Проблема: Ошибки в schema.yml

```bash
# Проверьте синтаксис тестов
# Для dbt 1.10+ используйте arguments: для generic тестов
# Пример:
# - accepted_values:
#     arguments:
#       values: ['value1', 'value2']
```

## Решение проблем с помощью Cursor или AI-агентов

### 1. Использование Cursor для отладки

#### Анализ ошибок dbt:
```bash
# В Cursor откройте терминал и запустите команды с флагом --verbose
dbt run --verbose
dbt test --verbose

# Скопируйте полный вывод ошибки и вставьте в чат Cursor
# AI поможет проанализировать ошибку и предложить решение
```

#### Поиск проблем в коде:
```bash
# Используйте Cursor для поиска проблем в SQL файлах
# Нажмите Ctrl+Shift+F и найдите:
# - "ref(" для поиска зависимостей
# - "source(" для поиска источников
# - "test" для поиска тестов
```

#### Автоматическое исправление:
```bash
# Cursor может автоматически исправить:
# - Синтаксические ошибки SQL
# - Проблемы с отступами в YAML
# - Неправильные ссылки на модели
```

### 2. Использование AI-агентов для решения проблем

#### Анализ логов dbt:
```bash
# Скопируйте полный лог ошибки
# Вставьте в AI-агент с контекстом:
# "У меня ошибка в dbt проекте: [вставить лог]"
# "Помогите найти и исправить проблему"
```

#### Генерация SQL запросов:
```bash
# Попросите AI сгенерировать SQL для проверки данных:
# "Создай SQL запрос для проверки, что в таблице mart_monthly_sales нет отрицательных значений total_sales"
```

#### Оптимизация производительности:
```bash
# Спросите AI о оптимизации:
# "Как оптимизировать dbt модель int_sales_orders для лучшей производительности?"
```

### 3. Типичные проблемы и решения

#### Проблема: "Model 'X' depends on a node named 'Y' which was not found"
```bash
# Решение:
# 1. Проверьте, что модель Y существует
# 2. Убедитесь, что используете правильный синтаксис ref() или source()
# 3. Проверьте, что модель Y в правильной схеме
```

#### Проблема: "Invalid exposures config"
```bash
# Решение:
# 1. Проверьте синтаксис YAML
# 2. Убедитесь, что тип exposure корректен
# 3. Проверьте отступы в файле
```

#### Проблема: "Deprecated functionality"
```bash
# Решение:
# 1. Обновите синтаксис тестов
# 2. Используйте arguments: для generic тестов
# 3. Проверьте документацию dbt для новых версий
```

## Полная пошаговая инструкция от копирования репозитория до публикации

### Шаг 1: Копирование репозитория

```bash
# Клонируйте репозиторий
git clone https://github.com/your-username/superstore_dwh_advanced.git

# Или скачайте ZIP архив и распакуйте
wget https://github.com/your-username/superstore_dwh_advanced/archive/main.zip
unzip main.zip
mv superstore_dwh_advanced-main superstore_dwh_advanced
```

### Шаг 2: Настройка окружения

```bash
# Создайте виртуальное окружение
python -m venv dbt-env

# Активируйте окружение
source dbt-env/bin/activate

# Установите dbt
pip install dbt-postgres
```

### Шаг 3: Настройка базы данных

```bash
# Подключитесь к PostgreSQL
psql -h localhost -U postgres

# Создайте базу данных (если не существует)
CREATE DATABASE superstore;

# Создайте схемы
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw_test;
CREATE SCHEMA IF NOT EXISTS dw_intermediate;
CREATE SCHEMA IF NOT EXISTS dw_snapshots;
```

### Шаг 4: Настройка проекта

```bash
# Перейдите в каталог проекта
cd superstore_dwh_advanced/

# Настройте profiles.yml
cp profiles.yml ~/.dbt/profiles.yml

# Проверьте подключение
dbt debug
```

### Шаг 5: Загрузка исходных данных

```bash
# Убедитесь, что у вас есть данные в схеме dw_test
# Если нет, запустите предыдущий проект superstore_dwh

# Проверьте наличие таблиц
psql -h localhost -U postgres -d superstore -c "\dt dw_test.*"
```

### Шаг 6: Запуск проекта

```bash
# Очистите кэш
dbt clean

# Запустите парсинг
dbt parse

# Запустите модели
dbt run

# Запустите тесты
dbt test

# Создайте снимки
dbt snapshot
```

### Шаг 7: Проверка результатов

```bash
# Проверьте созданные таблицы
psql -h localhost -U postgres -d superstore -c "\dt dw_intermediate.*"
psql -h localhost -U postgres -d superstore -c "\dt dw_test.*"
psql -h localhost -U postgres -d superstore -c "\dt dw_snapshots.*"

# Проверьте данные
psql -h localhost -U postgres -d superstore -c "SELECT COUNT(*) FROM dw_test.mart_monthly_sales;"
```

### Шаг 8: Генерация документации

```bash
# Сгенерируйте документацию
dbt docs generate

# Запустите локальный сервер
dbt docs serve

# Откройте браузер: http://localhost:8080
```

### Шаг 9: Публикация в dbt Cloud (опционально)

```bash
# 1. Зарегистрируйтесь на dbt Cloud
# 2. Создайте новый проект
# 3. Подключите репозиторий GitHub
# 4. Настройте подключение к базе данных
# 5. Запустите проект в dbt Cloud
```

### Шаг 10: Настройка CI/CD

```bash
# Создайте .github/workflows/dbt.yml
cat > .github/workflows/dbt.yml << 'EOF'
name: dbt CI/CD
on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  dbt:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.8'
    - name: Install dbt
      run: pip install dbt-postgres
    - name: Run dbt
      run: dbt run
EOF
```

## Перезапуск проекта

### Полный перезапуск:

```bash
# 1. Остановите все процессы
pkill -f "dbt docs serve"

# 2. Очистите кэш
dbt clean
rm -rf target/

# 3. Перезапустите проект
dbt run
dbt test
dbt snapshot
dbt docs generate
dbt docs serve
```

### Перезапуск после изменений:

```bash
# 1. Очистите кэш
dbt clean

# 2. Запустите только измененные модели
dbt run --select state:modified+

# 3. Запустите тесты для измененных моделей
dbt test --select state:modified+
```

### Перезапуск конкретной модели:

```bash
# Запустите конкретную модель и все ее зависимости
dbt run --select +mart_monthly_sales+

# Запустите тесты для конкретной модели
dbt test --select mart_monthly_sales
```

## Индивидуальное задание

**Задача**. Создать mart-модель `mart_valuable_dormant_customers` для определения "спящих" ценных клиентов.

**Бизнес-кейс**. Определить клиентов из топ-25% по общей выручке, которые не совершали покупок последние 6 месяцев.

**Решение**. Модель использует промежуточную модель `int_orders_pivoted` и применяет оконные функции для ранжирования клиентов по выручке, затем фильтрует тех, кто не совершал покупки последние 6 месяцев.
