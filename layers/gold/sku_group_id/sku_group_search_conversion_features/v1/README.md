# Gold Search Conversion Features по SKU Group ID

DAG id: `feature-platform.layers.gold.sku_group_id.sku_group_search_conversion_features`.

Пайплайн собирает финальные daily-признаки поисковой конверсии на уровне `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_sku_group_search_conversion_features`.

Задача слоя - обеспечить обратную совместимость с предыдущей моделью, где признаки строились из `query_skg_aggregated_conversions`, но на уровне `sku_group_id` без разреза по query.

Источники:

- `iceberg.silver.feature_platform_search_sku_group_id_install_query` - поисковые показы из daily search pre-aggregate;
- `iceberg.silver.feature_platform_sku_group_query_search_orders` - поисковые заказы по query и `sku_group_id`.

Окна считаются строго до даты расчета: для `ds = 2026-06-02` окно `3` использует даты `[2026-05-30, 2026-06-01]`. Сам `ds` не входит в расчет, как в старом SKU-level пайплайне.

Признаки:

- `smooth_conv_imp2order_3`;
- `smooth_conv_imp2order_7`;
- `smooth_conv_imp2order_14`;
- `conv_imp2order_3`;
- `conv_imp2order_7`;
- `conv_imp2order_14`;
- `imp2order_3_to_1`;
- `imp2order_21_to_14`;
- `imp2order_30_to_21`.

Формула сглаживания:

```text
(0.003384 + skg_uniq_orders_d) / (0.003384 + 1.402240 + skg_uniq_impressions_d)
```

Raw ratio-признаки считаются как отношение обычных конверсий:

```text
skg_conv_imp2order_d = skg_uniq_orders_d / skg_uniq_impressions_d
```

Raw conversion-признаки `conv_imp2order_3`, `conv_imp2order_7`, `conv_imp2order_14` используют ту же формулу без сглаживания, но возвращают `0.0` при нулевом знаменателе:

```text
conv_imp2order_d = 0.0, если skg_uniq_impressions_d = 0
conv_imp2order_d = skg_uniq_orders_d / skg_uniq_impressions_d иначе
```

Raw ratio-признаки `imp2order_*` сохраняют Spark-семантику `NULL` для нулевого или отсутствующего знаменателя. Это важно для совместимости признаков `imp2order_3_to_1`, `imp2order_21_to_14` и `imp2order_30_to_21` со старым feature-store пайплайном, где raw ratio не подменялись на `0.0`.

## Изменение схемы от 2026-06-17

Миграция `migrations/20260617_add_raw_conv_imp2order.sql` идемпотентно добавляет в существующую таблицу колонки `conv_imp2order_3`, `conv_imp2order_7` и `conv_imp2order_14`. Для новых окружений эти колонки также включены в `migrations/create_table.sql`.

После применения миграции Spark job записывает новые признаки вместе с остальными колонками таблицы. Сама миграция меняет только схему и не пересчитывает ранее записанные партиции: для исторических строк новые колонки остаются `NULL` до явного перезапуска соответствующих дат.

Пайплайн использует общий способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/gold/sku_group_id/sku_group_search_conversion_features/v1/entrypoints/get_sku_group_search_conversion_features.py`, поэтому отдельный Docker image для этой сущности не собирается.
