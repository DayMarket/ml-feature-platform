# 12-часовые product action counts

DAG id: `feature-platform.layers.silver.calculated_at_account_id_session_id_product_id_event_type.account_product_session_action_counts_12h`.

Airflow group tag: `recsys-main-page-features`.

Целевая таблица: `iceberg.silver.feature_platform_account_product_session_action_counts_12h`.

## Контракт

Grain и primary key: `calculated_at,account_id,session_id,product_id,event_type`.

- `calculated_at` — правая граница 12-часового интервала в UTC;
- `account_id` — положительный ID пользователя;
- `session_id` — ненулловый идентификатор сессии из события;
- `product_id` — положительный ID товара;
- `event_type` — `PRODUCT_VIEW`, `ADD_TO_CART` или `ADD_TO_FAVORITES`;
- `n_events` — количество исходных событий внутри группы;
- `last_received_at` — последнее UTC-время события внутри группы.

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
- last-clicked profile выбирает `PRODUCT_VIEW` за 30 дней, дедуплицирует account×session×product, сортирует по `last_received_at` и берёт последние 75 товаров;
- last-clicked discount присоединяет последний товар к S3 product price.

Потребители: account-product и account-category/brand/shop counts, product click/ATC/ATF popularity, recency, last-clicked profile, L1/L2 conversions и gender click-фичи.

## Оркестрация и хранение

DAG запускается в `07:00` и `19:00 UTC`, то есть в `12:00` и `00:00 Asia/Tashkent`. `calculated_at` берётся из UTC `data_interval_end`.

`start_date = 2026-06-01 00:00 UTC` задаёт нижнюю границу доступного исторического диапазона.
`catchup=false`, поэтому включение нового DAG не запускает исторический расчёт автоматически.
Перед подключением 30-дневных Gold-окон нужен отдельный контролируемый backfill.

Iceberg использует `hours(calculated_at)`, чтобы независимо и идемпотентно перезаписывать каждый 12-часовой срез через `overwritePartitions()`.

Подтверждённый upstream DQ DAG id внешней clickstream-таблицы неизвестен, поэтому отдельный sensor не добавлен.

CI создаёт стандартные dbt DQ-тесты: уникальность primary key и `not_null` для всех его колонок. `n_events > 0`, допустимый `event_type`, временные границы, положительные ID и границы `last_received_at` обеспечиваются самой трансформацией.

Ranking upload для Silver-контракта не настраивается.
