# Инструкция по запуску и проверке лабораторной работы №2

## 1. Что делает проект
Проект реализует ETL-пайплайн на Spark:

1. Загружает исходные данные `MOCK_DATA*.csv` в `PostgreSQL` (таблица `mock_data`).
2. Строит модель данных `звезда` в `PostgreSQL`:
   - `dim_customer`, `dim_seller`, `dim_product`, `dim_store`, `dim_supplier`, `dim_date`, `fact_sales`.
3. Строит 6 витрин в `ClickHouse`:
   - `mart_sales_by_products`
   - `mart_sales_by_customers`
   - `mart_sales_by_time`
   - `mart_sales_by_stores`
   - `mart_sales_by_suppliers`
   - `mart_product_quality`
4. Строит аналогичные 6 витрин в `Cassandra`.

## 2. Требования к окружению проверяющего
Нужно установить:

1. `Docker` + `Docker Compose`.
2. `Git`.
3. Bash-окружение для запуска скриптов (`Git Bash`, `WSL` или Linux/macOS shell).

Проверка:

```bash
docker --version
docker compose version
git --version
```

## 3. Подготовка проекта

1. Клонировать репозиторий и перейти в каталог проекта.
2. Проверить, что в папке `исходные данные` лежат 10 CSV-файлов `MOCK_DATA...`.
3. Создать `.env` из шаблона:

```bash
cp .env.example .env
```

Примечание: значения по умолчанию в `.env.example` уже подходят для запуска.

## 4. Быстрый запуск всего пайплайна

Из корня проекта выполнить:

```bash
bash jobs/scripts/run_pipeline.sh
```

Скрипт автоматически:

1. Поднимет контейнеры (`postgres`, `spark`, `clickhouse`, `cassandra`).
2. Дождется готовности БД.
3. Скачает JDBC-драйверы для PostgreSQL и ClickHouse в контейнер `spark`.
4. Установит Python-драйвер Cassandra (`cassandra-driver`) в контейнер `spark` (если он еще не установлен).
5. Запустит Spark-джобы:
   - `postgres_to_star.py`
   - `star_to_clickhouse.py`
   - `star_to_cassandra.py`
   - в `star_to_cassandra.py` выполняется DDL Cassandra: `CREATE KEYSPACE`, `CREATE TABLE`, `TRUNCATE`.
6. Выполнит базовые проверки количества строк и доступности витрин.

## 5. Ручной запуск по шагам (если нужен детальный контроль)

### 5.1 Поднять инфраструктуру

```bash
docker compose up -d
```

### 5.2 Скачать JDBC jar-файлы в контейнер Spark

```bash
docker exec -it spark bash -lc "cd /home/jovyan/work && bash ./scripts/download_jdbc.sh /home/jovyan/work/jdbc"
```


### 5.3 Важно для ручного запуска Cassandra-джобы

В текущей реализации `star_to_cassandra.py` сам подготавливает Cassandra-схему (создает keyspace/таблицы и делает `TRUNCATE`).
Для ручного запуска важно, чтобы в контейнере `spark` был установлен Python-драйвер Cassandra:

```bash
docker exec -it spark bash -lc "python -m pip install cassandra-driver==3.29.2"
```

### 5.4 Запустить Spark-джобы по очереди

```bash
docker exec -it spark bash -lc "cd /home/jovyan/work && python postgres_to_star.py"
docker exec -it spark bash -lc "cd /home/jovyan/work && python star_to_clickhouse.py"
docker exec -it spark bash -lc "cd /home/jovyan/work && python star_to_cassandra.py"
```

## 6. Как проверить, что витрины действительно построились

### 6.1 Проверка PostgreSQL

```bash
docker exec -it postgres psql -U user -d lab2 -c "SELECT COUNT(*) FROM mock_data;"
docker exec -it postgres psql -U user -d lab2 -c "SELECT COUNT(*) FROM fact_sales;"
docker exec -it postgres psql -U user -d lab2 -c "\dt"
```

Ожидание:

1. `mock_data` = `10000`.
2. `fact_sales` также `10000`.
3. Есть таблицы измерений и факт.

### 6.2 Проверка ClickHouse

```bash
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SHOW TABLES;"
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_sales_by_products;"
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_sales_by_customers;"
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_sales_by_time;"
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_sales_by_stores;"
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_sales_by_suppliers;"
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT count() FROM mart_product_quality;"
```

Ожидание:

1. Все 6 таблиц существуют.
2. `count()` в каждой витрине больше 0.

### 6.3 Проверка Cassandra

```bash
docker exec -it cassandra cqlsh -e "DESCRIBE KEYSPACES;"
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM lab2.mart_sales_by_products;"
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM lab2.mart_sales_by_customers;"
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM lab2.mart_sales_by_time;"
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM lab2.mart_sales_by_stores;"
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM lab2.mart_sales_by_suppliers;"
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM lab2.mart_product_quality;"
```

Ожидание:

1. Есть keyspace `lab2`.
2. Есть все 6 таблиц витрин.
3. В каждой таблице `COUNT(*) > 0`.

### 6.4 Вывод отчетов по витринам

Ниже запросы для вывода самих отчетов.

ClickHouse:

```bash
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT * FROM mart_sales_by_products ORDER BY sales_rank, product_key LIMIT 10;"
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT * FROM mart_sales_by_customers ORDER BY spend_rank, customer_key LIMIT 10;"
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT year_num, month_num, total_revenue, avg_order_amount FROM mart_sales_by_time ORDER BY year_num, month_num;"
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT * FROM mart_sales_by_stores ORDER BY revenue_rank, store_key LIMIT 10;"
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT * FROM mart_sales_by_suppliers ORDER BY revenue_rank, supplier_key LIMIT 10;"
docker exec -it clickhouse clickhouse-client --user user --password password -d lab2 -q "SELECT * FROM mart_product_quality ORDER BY avg_rating DESC, reviews_count DESC LIMIT 10;"
```

Cassandra (CQL):

```bash
docker exec -it cassandra cqlsh -e "SELECT * FROM lab2.mart_sales_by_products LIMIT 10;"
docker exec -it cassandra cqlsh -e "SELECT * FROM lab2.mart_sales_by_customers LIMIT 10;"
docker exec -it cassandra cqlsh -e "SELECT * FROM lab2.mart_sales_by_time LIMIT 12;"
docker exec -it cassandra cqlsh -e "SELECT * FROM lab2.mart_sales_by_stores LIMIT 10;"
docker exec -it cassandra cqlsh -e "SELECT * FROM lab2.mart_sales_by_suppliers LIMIT 10;"
docker exec -it cassandra cqlsh -e "SELECT * FROM lab2.mart_product_quality LIMIT 10;"
```

## 7. Остановка и очистка

Остановить контейнеры:

```bash
docker compose down
```

Остановить контейнеры и удалить тома (полная очистка данных):

```bash
docker compose down -v
```

## 8. Частые проблемы

1. `Permission denied` при запуске `.sh`:
   - запускать через `bash jobs/scripts/...`, а не напрямую.
2. Spark не может скачать пакеты:
   - проверить интернет и повторить запуск.
3. Порт уже занят:
   - освободить порты `5432`, `8888`, `8123`, `9000`, `9042`.
4. Сетевые сбои при загрузке зависимостей Spark/Cassandra:
   - иногда Maven/Ivy зависимости (Cassandra connector) скачиваются нестабильно;
   - если в логах есть `unresolved dependency`/`Connection timed out`, повторить запуск `bash jobs/scripts/run_pipeline.sh`.

## 9. Структура проекта (дерево)

```text
.
├── README.md
├── REPORT.md
├── docker-compose.yml
├── .env.example
├── postgres-init
│   ├── 01_init_mock_data.sql
│   └── 02_star_schema.sql
├── jobs
│   ├── common
│   │   ├── jdbc.py
│   │   └── spark_job.py
│   ├── scripts
│   │   ├── download_jdbc.sh  // скачивание jar-файлов
│   │   ├── run_pipeline.sh   // полный запуск цикла
│   ├── postgres_to_star.py
│   ├── source_mart_specs.py
│   ├── star_to_clickhouse.py
│   └── star_to_cassandra.py
└── исходные данные
    ├── MOCK_DATA.csv
    ├── MOCK_DATA (1).csv
    ├── ...
    └── MOCK_DATA (9).csv
```
