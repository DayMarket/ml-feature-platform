# 12-часовые показы account/L1

DAG id: `feature-platform.layers.silver.account_id_l1_category_id.account_l1_imp_counts_12h`.

Airflow group tag: `recsys-features`.

Целевая таблица: `iceberg.silver.feature_platform_account_l1_imp_counts_12h`.

## Контракт

Grain и primary key: `calculated_at,account_id,l1_category_id`.

- `calculated_at` — правая граница 12-часового интервала в `Asia/Tashkent`;
- `account_id` — положительный ID пользователя;
- `l1_category_id` — первый содержательный уровень категорийного пути;
- `n_impressions` — сумма session-level distinct product impressions.

Сырые product-level события в таблице не сохраняются.

## Источники и категории

Источники:

- `iceberg.silver_b2c_clickstream.events`;
- `iceberg.silver.product`;
- `iceberg.silver_apidb_kazanexpress.public_category`.

Категория определяется только по цепочке `event.product_id -> product.category_id -> category hierarchy`; `event.category_id` не используется. В агрегат записывается L1 — первый содержательный уровень от корня. Листовой `category_id` используется только внутри разрешения иерархии и не публикуется.

Технический root `category_id = 1` исключается. L1 берётся из полного `category.path`, поэтому глубина пути не ограничена количеством self-join. Уровни глубже L6 не публикуются и не меняют L1.

## Окно и distinct-семантика

Для каждого `calculated_at` читается полуинтервал:

```text
[calculated_at - 12 hours, calculated_at)
```

Фильтры:

- `event_type = 'PRODUCT_IMPRESSION'`;
- `account_id > 0`;
- `product_id > 0`;
- `session_id IS NOT NULL`;
- фильтра по `space` нет.

Сначала считается `COUNT(DISTINCT product_id)` внутри `account_id,session_id,l1_category_id`. Затем session-level значения суммируются до `account_id,l1_category_id`.

Один `session_id,product_id`, попавший в разные 12-часовые срезы, учитывается в каждом срезе отдельно.

## Оркестрация и хранение

DAG запускается в `07:00` и `19:00 UTC`, то есть в `12:00` и `00:00 Asia/Tashkent`. Для фильтрации событий `data_interval_end` используется как UTC-момент, а `calculated_at` записывается как соответствующее локальное время `Asia/Tashkent`.

`start_date = 2026-08-08 07:00 UTC`, `catchup=true`. Первый рассчитанный срез имеет `calculated_at = 2026-08-09 00:00 Asia/Tashkent`; при включении DAG 22 августа 2026 года это даёт начальный backfill за две недели.

Iceberg использует `days(calculated_at)`. Запись выполняется атомарным `MERGE` по primary key с удалением устаревших строк только конкретного `calculated_at`, поэтому два 12-часовых среза одного дня не перезаписывают друг друга. Spark использует профиль ресурсов `small`.

Подтверждённые upstream DQ DAG ids для внешних таблиц-источников неизвестны, поэтому отдельные sensors не добавлены.

После `MERGE` параллельно запускаются внутренние `dq` и `feature_stats` для конкретного `calculated_at`. DQ блокирует downstream при NULL или дублях primary key; freshness, минимальный объём и изменение объёма во время первичной раскатки имеют severity `warn`. `n_impressions > 0`, временные границы и положительные ID обеспечиваются самой трансформацией.

Внешние on-call алерты для DAG отключены. Ошибки остаются видимыми в статусах и логах Airflow.

`feature_stats` считает распределение `n_impressions` одним полным Trino-сканом записанного 12-часового среза на каждый запуск, то есть два скана в сутки. Ranking upload для Silver-контракта не настраивается.

`table.meta.create_dbt_pr: false`: CI не создаёт для таблицы новый DQ-PR в `dbt-trino`; Iceberg maintenance остаётся включённым.
