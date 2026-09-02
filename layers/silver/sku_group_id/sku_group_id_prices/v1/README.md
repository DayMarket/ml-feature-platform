# Silver-цены по SKU Group ID

DAG id: `feature-platform.layers.silver.sku_group_id.sku_group_id_prices`.

Пайплайн собирает дневную статистику цен на уровне `sku_group_id`.

Целевая таблица: `iceberg.silver.feature_platform_sku_group_id_prices`.

Основная логика:

- читает дневные цены из `iceberg.silver.sku_eod`;
- обогащает записи SKU-данными из `iceberg.silver.sku`;
- фильтрует `sku_eod` по `dt = ds`;
- агрегирует среднюю, медианную, минимальную и максимальную цену продажи на конец дня;
- агрегирует среднюю, медианную, минимальную и максимальную полную цену на конец дня;
- пишет результат в Iceberg через `overwritePartitions()`.

Миграции:

- `create_table.sql` создает актуальную схему таблицы для новых окружений;
- `20260602_add_price_min_max_columns.sql` добавляет признаки `min_sell_price_eod`, `max_sell_price_eod`, `min_full_price_eod`, `max_full_price_eod` через `ALTER TABLE`;
- job проверяет наличие новых колонок и применяет missing-column migration перед формированием датафрейма признаков.

DAG запускается ежедневно в `01:00 UTC` и ждет DAG `dbt.models.dwh_trino.sku_eod`, который запускается в `00:00 UTC`.

## DQ

DQ-тесты выполняются таской `dq` внутри этого DAG'а сразу после записи партиции; каталог
тестов и правила конфигурирования описаны в `dq/README.md`.

Базовый набор: `primary_key_not_null`, `primary_key_unique`, `row_count_min`,
`row_count_growth`, `freshness`.

Дополнительно: `non_negative` по всем ценовым колонкам и два `expression_is_true`,
проверяющих упорядоченность `min <= median <= max` отдельно для sell- и full-цен.

`row_count_growth` временно имеет `severity: warn`: двусторонняя проверка роста впервые
начинает ловить обвалы объёма, которых прежний односторонний dbt-тест не видел. Поднять до
`error` после двух недель наблюдений.

Downstream ждёт таску `dq` этого DAG'а, а не dbt-DQ-DAG.
