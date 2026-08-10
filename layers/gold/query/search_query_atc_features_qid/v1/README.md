# iceberg.gold.feature_platform_search_query_atc_features_qid

Фичи показов, ATC и заказов по поисковому запросу, посчитанные в рамках каноничного `query_id`
и развёрнутые обратно на исходные `query_text`.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_search_query_atc_features_qid`.
- DAG: `feature-platform.layers.gold.query.search_query_atc_features_qid`
  (`layers/gold/query/search_query_atc_features_qid/v1/dag.py`).
- Расписание: ежедневно, `0 6 * * *` UTC, после DAG справочника `query_id` (`0 5 * * *`).
- `start_date=2026-08-11T00:00:00Z`, `is_paused_upon_creation=True`.

## Грейн / ключ

`date, query`. `query` — исходный нормализованный поисковый запрос, а не `query_id`: ключ джойна
в ranking-сервисе не меняется.

## Источники

- `iceberg.silver.feature_platform_search_sku_group_id_install_query` - показы и ATC, колонка
  `uniqs` при `space = 'SEARCH_RESULTS'`.
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

Отличие от `feature_platform_search_query_atc_features` ровно одно: ключ агрегации. Имена фичей,
формулы, окна (1, 3, 7, 14, 21, 30, 60, 90) и границы окон `[ds - N, ds - 1]` совпадают дословно.

1. Запрос нормализуется: `lower` -> `ё` в `е` -> схлопывание пробелов -> `trim`. Та же нормализация
   применяется к `query_text` из справочника, потому что справочник хранит сырой текст.
2. `group_key = coalesce(query_id, query)`. Запрос, которого нет в справочнике, образует группу из
   самого себя, поэтому покрытие выхода не теряется.
3. Оконные суммы считаются по `group_key`.
4. Результат разворачивается обратно: каждый `query_text` группы получает значения своей группы.

Число строк совпадает с `feature_platform_search_query_atc_features`: универсум запросов тот же,
меняются только значения.

## Колонки сверх зеркала

- `query_id` - ключ группы, по которой посчитана строка. При фолбэке равен самому `query`.
- `has_query_id` - `false` для фолбэка.

Обе колонки служебные, в вектор ranking upload не входят.

## Рантайм

Shared Spark-образ и `git-sync`, шаблон `config/spark/layer_spark_application.yaml`, профиль
ресурсов `small`. Запись - `overwritePartitions()` по `date`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
