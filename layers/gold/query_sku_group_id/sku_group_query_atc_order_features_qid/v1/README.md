# iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features_qid

Pairwise-фичи ATC и заказных конверсий по паре запрос-sku_group_id, посчитанные в рамках
каноничного `query_id` и развёрнутые обратно на исходные `query_text`.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features_qid`.
- DAG: `feature-platform.layers.gold.query_sku_group_id.sku_group_query_atc_order_features_qid`
  (`layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1/dag.py`).
- Расписание: ежедневно, `0 6 * * *` UTC, после DAG справочника `query_id` (`0 5 * * *`).
- `start_date=2026-08-11T00:00:00Z`, `is_paused_upon_creation=True`.

## Грейн / ключ

`date, query, sku_group_id`. `query` - исходный нормализованный поисковый запрос, а не `query_id`.

## Источники

- `iceberg.silver.feature_platform_search_sku_group_id_install_query` - показы и ATC при
  `space = 'SEARCH_RESULTS'`.
- `iceberg.silver.feature_platform_sku_group_query_search_orders` - сгенерированные заказы.
- `iceberg.gold.feature_platform_search_query_id` - справочник каноничных `query_id`, `version = 'v1'`.

## Зависимости

- `feature-platform.layers.gold.query_text_version.search_query_id` (`execution_delta = 1 час`) -
  сам DAG справочника, не его DQ.
- `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq`
  (`execution_delta = 5 часов`).
- `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_query_search_orders.dq`
  (`execution_delta = 5 часов`).

## Логика

Отличие от `feature_platform_search_sku_group_id_query_atc_order_features_v2` ровно одно: ключ
парной агрегации. Имена фичей, формулы, окна (1, 3, 7, 14, 21, 30, 60, 90), коэффициент сглаживания
100 и границы окон совпадают дословно.

1. Запрос нормализуется: `lower` -> `ё` в `е` -> схлопывание пробелов -> `trim`. Та же нормализация
   применяется к `query_text` из справочника.
2. `group_key = coalesce(query_id, query)`.
3. Парные суммы считаются по `group_key, sku_group_id`. Знаменатели уровня sku
   (`skg_smooth_atcs_*`, `skg_smooth_orders_*`) остаются на `sku_group_id` и не меняются.
4. Фильтры `query_skg_uniq_impressions_14 >= 2` и `query_skg_uniq_atcs_90 > 0 OR
   query_skg_uniq_orders_90 > 0` применяются на уровне группы, до разворота.
5. Разворот: каждый `query_text` группы получает все пары группы, прошедшие фильтры, включая пары
   с sku, которые с этим конкретным запросом раньше не встречались. Это и есть уплотнение.

Число строк растёт: на замере от 2026-08-09 разворот даёт около 3.3 раза к v2 при среднем размере
группы 1.30, и растёт дальше по мере наполнения справочника. Отсечки топ-N нет, пишутся все пары.

## Колонки сверх зеркала

- `query_id` - ключ группы, по которой посчитана строка. При фолбэке равен самому `query`.
- `has_query_id` - `false` для фолбэка.

Обе колонки служебные, в вектор ranking upload не входят.

## Рантайм

Shared Spark-образ и `git-sync`, шаблон `config/spark/layer_spark_application.yaml`, профиль
ресурсов `large`. Запись - `overwritePartitions()` по `date`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
