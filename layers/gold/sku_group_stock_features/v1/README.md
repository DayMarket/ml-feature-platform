# Gold-фичи остатков по SKU Group

Пайплайн строит дневные признаки активного EOD-остатка на уровне `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_sku_group_stock_features`.

Grain и primary key: `date, sku_group_id`.

Основная логика:

- читает дневные остатки SKU из `iceberg.silver.feature_platform_sku_stock_daily`;
- читает маппинг SKU в SKU Group из `iceberg.silver.sku`;
- джойнится по `sku_id = id`;
- исключает строки маппинга без `sku_group_id`;
- агрегирует `total_stock` из SKU-level источника до дневного уровня `date, sku_group_id`;
- считает окна 1, 3, 7, 14, 21, 30, 60 и 90 дней;
- окна считаются как `[ds - n, ds - 1]`, то есть дата расчёта не включается;
- пишет результат в Iceberg через `overwritePartitions()`.

Выходные признаки:

- `skg_total_stock_{1,3,7,14,21,30,60,90}`.

Формула:

```text
skg_total_stock_n = SUM(total_stock)
```

где сумма берется по всем SKU внутри `sku_group_id` и по всем дневным партициям окна `[ds - n, ds - 1]`.

DAG ждет DQ DAG silver-источника:

- `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_stock_daily.dq`.

`iceberg.silver.sku` используется как справочник маппинга `sku_id -> sku_group_id`; отдельный DQ sensor для него в этом DAG не настроен, как и в соседних feature-platform jobs, которые используют этот справочник.

Пайплайн использует общий способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/gold/sku_group_stock_features/v1/entrypoints/get_sku_group_stock_features.py`, поэтому отдельный Docker image для этой сущности не собирается.

Ranking upload для этой таблицы не настроен.
