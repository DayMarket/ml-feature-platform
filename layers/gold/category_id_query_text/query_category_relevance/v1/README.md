# iceberg.gold.feature_platform_query_category_relevance

Метка релевантности категории-кандидата поисковому запросу, одна формулировка на
`query_id`. Витрина наполняется отдельным процессом и напрямую в ranking-service не
выгружается: её читает
`iceberg.gold.feature_platform_query_category_relevance_expanded`
([README](../../query_category_relevance_expanded/v1/README.md)), которая раскладывает
метку на все формулировки `query_id` и уже публикуется.

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
  (общий с `query_category_relevance_expanded` и upload'ом).
- Расписание: ежедневно в 03:00 UTC, `0 3 * * *`, `start_date=2026-09-14T00:00:00Z`,
  `catchup=False`, `max_active_runs=1`, DAG создаётся на паузе.
- Сенсоров нет: у заглушки нет источников.
- Таска `materialize`. DAG
  `feature-platform.layers.gold.category_id_query_text.query_category_relevance_expanded`
  ждёт именно её, поэтому при замене заглушки настоящим job'ом `task_id` сохраняется.

## DQ и feature_stats

Тасок `dq` и `feature_stats` в DAG'е нет — осознанное решение владельца от 2026-09-13:
DAG — заглушка без записи партиций, и на пустой таблице базовые `freshness` и
`row_count_min` падали бы каждый день. Поэтому downstream-DAG ждёт таску `materialize`,
а не `dq`. Когда здесь появится `build_dq_task`, сенсор в `query_category_relevance_expanded`
переключается на `dq`.

## Downstream

`query_category_relevance_expanded` читает всю витрину (`date <= даты прогона`),
добавляет все формулировки `query_id` из `iceberg.gold.feature_platform_search_query_id`,
приводит `query_text` к нижнему регистру и оставляет одну строку на пару
`category_id, query_text`. В ranking-service уходит уже она.

## Требования к будущему job'у

- `query_text` нормализуется так же, как строка запроса, с которой ranking-service
  ищет признак: иначе ключи не совпадут и модель получит `missing-feature-value`.
- `relevance` принимает только `0`, `1`, `2` или NULL.
- Одна строка на `date, category_id, query_text`.
- Появление job'а — повод вернуться к решению о `dq` и `feature_stats`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
