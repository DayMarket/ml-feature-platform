# SKU Group Median Sales 7D

Gold-таблица с медианным суточным количеством завершенных продаж по `sku_group_id` за последние 7 суток.

Пайплайн запускается раз в три часа и использует `data_interval_end` как правую границу окна. Для каждого активного `sku_group_id` продажи раскладываются в семь 24-часовых бакетов, отсутствующие бакеты заполняются нулем, после чего считается `median_sales_count_7d`.

Таблица создается не внутри Spark job. Миграция `migrations/create_table.sql` выполняется CI-процессом после мерджа в `master`, поэтому runtime job только рассчитывает признаки и пишет партицию.

В отличие от большинства сущностей репозитория, этот пайплайн использует новый способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/gold/sku_group_median_sales_7d/v1/entrypoints/get_sku_group_median_sales_7d.py`, поэтому отдельный Docker image для этой сущности не собирается.
