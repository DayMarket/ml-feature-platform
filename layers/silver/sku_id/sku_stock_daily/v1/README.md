# Silver-остатки по SKU

DAG id: `feature-platform.layers.silver.sku_id.sku_stock_daily`.

Пайплайн собирает дневной признак активного остатка на уровне `sku_id`.

Целевая таблица: `iceberg.silver.feature_platform_sku_stock_daily`.

Грейн и primary key: `date, sku_id`.

Основная логика:

- читает дневные остатки из `iceberg.silver.sku_eod`;
- фильтрует источник по `dt = date`, где `date` берется из Airflow `data_interval_start` в UTC;
- исключает строки без `sku_id`;
- считает `total_stock = SUM(quantity_active_eod)` по `sku_id`;
- пишет результат в Iceberg через `overwritePartitions()`.

Партиция результата соответствует `dt` источника `sku_eod`.

DAG запускается ежедневно в `00:00 UTC` и ждет DAG `dbt.models.dwh_trino.sku_eod` с тем же logical date.

Пайплайн использует общий способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/silver/sku_id/sku_stock_daily/v1/entrypoints/get_sku_stock_daily.py`, поэтому отдельный Docker image для этой сущности не собирается.

Ranking-upload не настроен: текущий upload-контракт не поддерживает entity key `sku_id`.
