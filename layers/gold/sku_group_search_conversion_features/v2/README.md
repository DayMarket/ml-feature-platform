# Gold Search Conversion Features по SKU Group ID

Пайплайн собирает финальные daily-признаки поисковой конверсии на уровне `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_sku_group_search_conversion_features_v2`.

Задача слоя - обеспечить обратную совместимость с предыдущей моделью, где признаки строились из `query_skg_aggregated_conversions`, но на уровне `sku_group_id` без разреза по query.

Источники:

- `iceberg.silver.feature_platform_search_sku_group_id_install_query` - поисковые показы из daily search pre-aggregate;
- `iceberg.silver.feature_platform_sku_group_query_search_orders` - поисковые заказы по query и `sku_group_id`;
- `iceberg.silver.sku` - маппинг `sku_group_id` в `category_id`.

Окна считаются строго до даты расчета: для `ds = 2026-06-02` окно `3` использует даты `[2026-05-30, 2026-06-01]`. Сам `ds` не входит в расчет, как в старом SKU-level пайплайне.

Признаки:

- `smooth_conv_imp2order_3`;
- `smooth_conv_imp2order_7`;
- `smooth_conv_imp2order_14`;
- `conv_imp2order_3`;
- `conv_imp2order_7`;
- `conv_imp2order_14`;
- `skg_days_since_last_impression`;
- `skg_days_since_last_atc`;
- `skg_conv_atc2order_{1,3,7,14,21,30,60,90}`;
- `skg_return_rate_{1,3,7,14,21,30,60,90}`;
- `skg_incr_rate_orders_1_to_{3,7,14,21,30,60,90}`;
- `skg_incr_rate_atc_1_to_{3,7,14,21,30,60,90}`;
- `category_id`;
- `category_skg_orders_{1,3,7,14,21,30,60,90}`;
- `category_orders_{1,3,7,14,21,30,60,90}`;
- `category_skg_atc_{1,3,7,14,21,30,60,90}`;
- `category_atc_{1,3,7,14,21,30,60,90}`;
- `category_skg_orders_frac_category_orders_{1,3,7,14,21,30,60,90}`;
- `category_skg_atc_frac_category_atc_{1,3,7,14,21,30,60,90}`;
- `imp2order_3_to_1`;
- `imp2order_21_to_14`;
- `imp2order_30_to_21`.

Формула сглаживания:

```text
(0.003384 + skg_uniq_orders_d) / (0.003384 + 1.402240 + skg_uniq_impressions_d)
```

Внутренние raw ratio-признаки считаются как отношение обычных конверсий для окон 1, 3, 7, 14, 21, 30, 60 и 90 дней:

```text
skg_conv_imp2order_d = skg_uniq_orders_d / skg_uniq_impressions_d
```

Raw conversion-признаки `conv_imp2order_3`, `conv_imp2order_7`, `conv_imp2order_14` используют ту же формулу без сглаживания, но возвращают `0.0` при нулевом знаменателе:

```text
conv_imp2order_d = 0.0, если skg_uniq_impressions_d = 0
conv_imp2order_d = skg_uniq_orders_d / skg_uniq_impressions_d иначе
```

ATC-to-order признаки считаются для окон 1, 3, 7, 14, 21, 30, 60 и 90 дней:

```text
skg_conv_atc2order_d = skg_uniq_orders_d / skg_uniq_atcs_d
```

где `skg_uniq_atcs_d` строится из `sum_atc` в `iceberg.silver.feature_platform_search_sku_group_id_install_query`, а `skg_uniq_orders_d` — из `orders_generated` в `iceberg.silver.feature_platform_sku_group_query_search_orders`. При нулевом или отсутствующем знаменателе сохраняется Spark-семантика `NULL`.

Return rate признаки считаются для окон 1, 3, 7, 14, 21, 30, 60 и 90 дней:

```text
skg_return_rate_d = skg_returned_orders_d / skg_uniq_orders_d
```

где `skg_returned_orders_d` строится из `returned_orders`, а `skg_uniq_orders_d` — из `orders_generated` в `iceberg.silver.feature_platform_sku_group_query_search_orders`. При нулевом или отсутствующем знаменателе сохраняется Spark-семантика `NULL`.

Increment rate признаки показывают долю вчерашней активности в накопленном окне и считаются для окон 3, 7, 14, 21, 30, 60 и 90 дней:

```text
skg_incr_rate_orders_1_to_d = skg_uniq_orders_1 / skg_uniq_orders_d
skg_incr_rate_atc_1_to_d = skg_uniq_atcs_1 / skg_uniq_atcs_d
```

Окно `1_to_1` не выводится, так как оно сводится к `x / x`. При нулевом или отсутствующем знаменателе сохраняется Spark-семантика `NULL`.

Category-признаки используют `category_id` из `iceberg.silver.sku`. Если для одного `sku_group_id` найдено несколько `category_id`, job выбирает максимальный `category_id`, чтобы сохранить grain таблицы `date, sku_group_id` и не размножать строки.

```text
category_skg_orders_d = skg_uniq_orders_d
category_skg_atc_d = skg_uniq_atcs_d
category_orders_d = sum(category_skg_orders_d) по всем sku_group_id этой category_id
category_atc_d = sum(category_skg_atc_d) по всем sku_group_id этой category_id
category_skg_orders_frac_category_orders_d = category_skg_orders_d / category_orders_d
category_skg_atc_frac_category_atc_d = category_skg_atc_d / category_atc_d
```

При отсутствующем `category_id` category-level признаки остаются `NULL`. При нулевом или отсутствующем знаменателе в долях сохраняется Spark-семантика `NULL`.

Внутренние raw ratio-признаки `skg_conv_imp2order_*` сохраняют Spark-семантику `NULL` для нулевого или отсутствующего знаменателя. В финальный select они не выводятся напрямую, но используются для расчета признаков `imp2order_3_to_1`, `imp2order_21_to_14` и `imp2order_30_to_21`. Это важно для совместимости со старым feature-store пайплайном, где raw ratio не подменялись на `0.0`.

Recency-признаки считаются по поисковым событиям `space = 'SEARCH_RESULTS'` из `iceberg.silver.feature_platform_search_sku_group_id_install_query` в окне `[ds - 90, ds - 1]`:

```text
skg_days_since_last_impression = ds - max(date), где sum_impressions > 0
skg_days_since_last_atc = ds - max(date), где sum_atc > 0
```

Если соответствующего события в 90-дневном окне нет, значение остается `NULL`. Recency-признаки присоединяются к существующему набору `sku_group_id` этой витрины через `LEFT JOIN`, чтобы не расширять row-set таблицы, которая уже используется в ranking upload для существующих conversion-фичей.

Версия `v2` создается как новая Iceberg-таблица. Вся схема, включая старые conversion-признаки и новые recency, atc2order, return-rate, increment-rate и category-признаки, описана в `migrations/create_table.sql`; отдельных schema-change миграций для добавления фичей в этой версии нет.

Пайплайн использует общий способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/gold/sku_group_search_conversion_features/v2/entrypoints/get_sku_group_search_conversion_features.py`, поэтому отдельный Docker image для этой сущности не собирается.
