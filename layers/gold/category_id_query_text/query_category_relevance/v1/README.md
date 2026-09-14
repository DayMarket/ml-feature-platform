# iceberg.gold.feature_platform_query_category_relevance

Метка релевантности категории-кандидата поисковому запросу. Витрина — источник набора
признаков `query_category_relevance` модели `search_unified_model_clusters`
в ranking-service (коллекция `SKU_GROUP_CATEGORY_TO_QUERY`).

**Статус: заглушка.** Таблица создаётся миграцией, DAG запускается ежедневно, но job'а
ещё нет: единственная таска `materialize` — `EmptyOperator`, партиции не пишутся.

## Выход

- Таблица: `iceberg.gold.feature_platform_query_category_relevance`.
- Ключ: `date, category_id, query_text`; партиционирование по `date`.

| Колонка | Тип | Обязательная | Описание |
|---|---|---|---|
| `date` | `DATE` | да | дата партиции (UTC) |
| `query_id` | `STRING` | нет | ID поискового запроса (в Trino — `varchar`) |
| `query_text` | `STRING` | да | текст поискового запроса |
| `category_id` | `BIGINT` | да | категория-кандидат |
| `relevance` | `INT` | нет | метка релевантности `0` / `1` / `2` |

## Оркестрация

- DAG id: `feature-platform.layers.gold.category_id_query_text.query_category_relevance`
  (`layers/gold/category_id_query_text/query_category_relevance/v1/dag.py`).
- Групповой тег Airflow: `dag.group_tag = query-category-relevance`
  (общий с upload `feature-platform.upload.query_category_relevance_upload`).
- Расписание: ежедневно в 03:00 UTC, `0 3 * * *`, `start_date=2026-09-14T00:00:00Z`,
  `catchup=False`, `max_active_runs=1`, DAG создаётся на паузе.
- Сенсоров нет: у заглушки нет источников.
- Таска `materialize`. Upload ждёт именно её, поэтому при замене заглушки настоящим
  job'ом `task_id` сохраняется.

## DQ и feature_stats

Тасок `dq` и `feature_stats` в DAG'е нет — осознанное решение владельца от 2026-09-13:
DAG — заглушка без записи партиций, и на пустой таблице базовые `freshness` и
`row_count_min` падали бы каждый день. Из-за этого upload ждёт таску `materialize`,
а не `dq`, и объявляет это исключение полем `source.dq_waiver_reason`.
`scripts/validate_ranking_upload_configs.py` отклонит исключение, как только в этом
`dag.py` появится `build_dq_task`: тогда сенсор upload'а переключается на `dq`,
а поле удаляется.

## Выгрузка

Upload `feature-platform.upload.query_category_relevance_upload`
(`upload/query_category_relevance_upload/v1`) публикует всю таблицу, а не одну
партицию. Строки дополняются всеми формулировками того же `query_id` из
`iceberg.gold.feature_platform_search_query_id`, `query_text` приводится к нижнему
регистру, и по каждой паре `category_id, query_text` берётся строка с самой свежей `date`,
а при равной `date` — с максимальным `relevance`.
`query_text` уходит ключом `query`, `category_id` — ключом `skuGroupCategoryId`,
`relevance` — единственным признаком (NULL отправляется как `0.0`).

## Требования к будущему job'у

- `query_text` нормализуется так же, как строка запроса, с которой ranking-service
  ищет признак: иначе ключи не совпадут и модель получит `missing-feature-value`.
- `relevance` принимает только `0`, `1`, `2` или NULL.
- Одна строка на `date, category_id, query_text`.
- Появление job'а — повод вернуться к решению о `dq` и `feature_stats`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
