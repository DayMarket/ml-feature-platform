# 12-часовые product action counts

DAG id: `feature-platform.layers.silver.account_id_session_id_product_id_event_type.account_product_session_action_counts_12h`.

Airflow group tag: `recsys-main-page-features`.

Целевая таблица: `iceberg.silver.feature_platform_account_product_session_action_counts_12h`.

## Контракт

Grain и primary key: `calculated_at,account_id,session_id,product_id,event_type`.

- `calculated_at` — правая граница 12-часового интервала в `Asia/Tashkent`;
- `account_id` — положительный ID пользователя;
- `session_id` — ненулловый идентификатор сессии из события;
- `product_id` — положительный ID товара;
- `event_type` — `PRODUCT_VIEW`, `ADD_TO_CART` или `ADD_TO_FAVORITES`;
- `n_events` — количество исходных событий внутри группы;
- `last_received_at` — последнее время события внутри группы в `Asia/Tashkent`.

Product impressions и сырые event-level строки в таблице не сохраняются.

## Источник и расчёт

Источник: `iceberg.silver_b2c_clickstream.events`.

Для каждого `calculated_at` читается полуинтервал:

```text
[calculated_at - 12 hours, calculated_at)
```

Фильтры:

- `event_type IN ('PRODUCT_VIEW', 'ADD_TO_CART', 'ADD_TO_FAVORITES')`;
- `account_id > 0`;
- `product_id > 0`;
- `session_id IS NOT NULL`;
- фильтра по `space` нет, учитываются события по всему приложению.

Внутри `account_id,session_id,product_id,event_type` вычисляются `COUNT(*)` и `MAX(received_at)`.

## Семантика потребления

- product popularity суммирует `n_events`;
- account event counts дедуплицирует `account_id,session_id,product_id,event_type` внутри полного rolling-окна и не суммирует `n_events`;
- L1/L2 conversion numerators присоединяют строки к S1 по `product_id`;
- recency использует `MAX(last_received_at)`;
- last-clicked profile выбирает `PRODUCT_VIEW` за 28 дней, дедуплицирует account×session×product, сортирует по `last_received_at` и берёт последние 75 товаров;
- last-clicked discount присоединяет последний товар к S3 product price.

Потребители: account-product и account-category/brand/shop counts, product click/ATC/ATF popularity, recency, last-clicked profile, L1/L2 conversions и gender click-фичи.

## Оркестрация и хранение

DAG запускается в `07:00` и `19:00 UTC`, то есть в `12:00` и `00:00 Asia/Tashkent`. Для фильтрации событий `data_interval_end` используется как UTC-момент; `calculated_at` и `last_received_at` записываются в локальном времени `Asia/Tashkent`.

`start_date = 2026-08-08 07:00 UTC`, `catchup=true`. Первый рассчитанный срез имеет `calculated_at = 2026-08-09 00:00 Asia/Tashkent`; при включении DAG 22 августа 2026 года это даёт начальный backfill за две недели.

Iceberg использует `days(calculated_at)`. Запись выполняется атомарным `MERGE` по primary key с удалением устаревших строк только конкретного `calculated_at`, поэтому два 12-часовых среза одного дня не перезаписывают друг друга. Spark использует профиль ресурсов `small`.

Подтверждённый upstream DQ DAG id внешней clickstream-таблицы неизвестен, поэтому отдельный sensor не добавлен.

CI создаёт стандартные dbt DQ-тесты: уникальность primary key и `not_null` для всех его колонок. `n_events > 0`, допустимый `event_type`, временные границы, положительные ID и границы `last_received_at` обеспечиваются самой трансформацией.

Ranking upload для Silver-контракта не настраивается.
