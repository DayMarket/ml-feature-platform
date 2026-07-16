# Gold-фичи ATC и заказов по Query и SKU Group

Страница описывает различия между версиями таблицы `sku_group_query_atc_order_features`.
Обе версии строят дневной срез на грейне `date, query, sku_group_id` из одних и тех же silver-источников:

- `iceberg.silver.feature_platform_search_sku_group_id_install_query`;
- `iceberg.silver.feature_platform_sku_group_query_search_orders`.

Обе версии используют одинаковую нормализацию `query`, фильтр `space = SEARCH_RESULTS`, основу ключей из поисковых показов, `LEFT JOIN` заказов, фильтр `query_skg_uniq_impressions_14 >= 2`, отсечение пар без ATC и заказов за 90 дней, Spark `overwritePartitions()` и те же DQ sensors silver-источников.

## Версии

| Версия | README | DAG id | Целевая таблица |
| --- | --- | --- | --- |
| `v1` | [`v1/README.md`](v1/README.md) | `feature-platform.layers.gold.query_sku_group_id.sku_group_query_atc_order_features` | `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features` |
| `v2` | [`v2/README.md`](v2/README.md) | `feature-platform.layers.gold.query_sku_group_id.sku_group_query_atc_order_features.v2` | `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features_v2` |

## Кратко: v1 vs v2

`v1` содержит базовый набор pairwise-признаков ATC и заказов:

- количество сгенерированных заказов за окна 7, 14, 21, 30, 60 и 90 дней;
- конверсии `impression -> atc` и `impression -> order` за окна 7, 14, 21, 30, 60 и 90 дней;
- отношения конверсий между соседними окнами, например `query_skg_imp2atc_7_to_3` и `query_skg_imp2order_60_to_30`;
- ATC-счетчики за 60 и 90 дней.

`v2` является отдельной Iceberg-таблицей и сохраняет базовый набор `v1`, но добавляет признаки, которые учитывают поведение всего `sku_group_id` по всем поисковым запросам:

- `query_skg_smooth_conv_imp2atc_{1,3,7,14,21,30,60,90}`;
- `query_skg_smooth_conv_imp2order_{1,3,7,14,21,30,60,90}`;
- `query_skg_atc_frac_all_skg_atc_{1,3,7,14,21,30,60,90}`;
- `query_skg_orders_frac_all_skg_orders_{1,3,7,14,21,30,60,90}`.

Сглаженные признаки в `v2` считаются с коэффициентом `100`:

```text
(query_skg_metric_n + 100 * skg_conversion_n) / (query_skg_impressions_n + 100)
```

Для ATC `skg_conversion_n` строится как `skg_atc_n / skg_impressions_n`, для заказов — как `skg_orders_n / skg_impressions_n`. Значения `skg_*` агрегируются на уровне `sku_group_id` по всем поисковым запросам за то же окно.

Долевые признаки в `v2` показывают вклад пары `query, sku_group_id` во все ATC или заказы этого `sku_group_id` за окно:

```text
query_skg_atc_frac_all_skg_atc_n = query_skg_atc_n / skg_atc_n
query_skg_orders_frac_all_skg_orders_n = query_skg_orders_n / skg_orders_n
```

Добавленные в `v2` smoothed/fraction-признаки используют окна `[ds - n, ds - 1]`, то есть не включают дату расчета. Базовые conversion/count/ratio-признаки, унаследованные из `v1`, оставлены с прежней логикой расчета.

## Практический вывод

`v1` подходит как базовая версия с уже существующим набором pairwise conversion-признаков. `v2` нужна, когда потребителю важны более устойчивые признаки для редких пар `query, sku_group_id`: она добавляет priors на уровне `sku_group_id` и доли пары внутри всех ATC/заказов sku group.

Переход с `v1` на `v2` не является schema evolution существующей таблицы: у `v2` отдельные DAG, Spark application, table key и физическая Iceberg-таблица. Потребитель должен явно переключиться на таблицу `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features_v2`.

На момент описания текущий ranking upload `fs_search_query_skg_v3` в `upload/features_service_upload/v1/config.yaml` читает legacy-таблицу `feature_platform_query_skg_pairwise_features_legacy`, а не `v1` или `v2` этой сущности.
