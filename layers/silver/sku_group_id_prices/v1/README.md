# Silver-цены по SKU Group ID

Пайплайн собирает дневную статистику цен на уровне `sku_group_id`.

Целевая таблица: `iceberg.silver.feature_platform_sku_group_id_prices`.

Основная логика:

- читает дневные цены из `iceberg.silver.sku_eod`;
- обогащает записи SKU-данными из `iceberg.silver.sku`;
- фильтрует `sku_eod` по `dt = ds`;
- агрегирует среднюю и медианную цену продажи на конец дня;
- агрегирует среднюю и медианную полную цену на конец дня;
- пишет результат в Iceberg через `overwritePartitions()`.

DAG запускается ежедневно в `01:00` и ждет DAG `dbt.models.dwh_trino.sku_eod`, который запускается в `00:00`.
