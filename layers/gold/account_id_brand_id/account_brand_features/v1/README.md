# Account-brand features

DAG id: `feature-platform.layers.gold.account_id_brand_id.account_brand_features`.

Airflow group tag: `recsys-features`.

Целевая таблица: `iceberg.gold.feature_platform_account_brand_features`.

## Контракт

Путь сущности: `layers/gold/account_id_brand_id/account_brand_features/v1`.

Grain и primary key: `calculated_at,account_id,brand_id`.

Идентификаторы и счётчики в физическом контракте имеют тип `INT`.
Namespace контракта — `ACCOUNT_BRAND`. Все физические feature-колонки уже
содержат его, например `ACCOUNT_BRAND__n_clicks_7d`; устаревший префикс `bid_`
не используется. Ключи `calculated_at`, `account_id` и `brand_id` остаются без
namespace.

`calculated_at` — граница Gold snapshot: `00:00` или `12:00 Asia/Tashkent`.
Публикуются только содержательные `brand_id`: `NULL` и business placeholder
`160078`, уже нормализованный S1 в `NULL`, в результат не попадают.

Колонки:

- `n_clicks_{3,7,14,28}d`;
- `gmv_{3,7,14,28,60,90}d`;
- `gmv_{3,7,14,28,60,90}d_ratio`.

## Clicks

Источники:

- `iceberg.silver.feature_platform_account_product_session_action_counts_12h`;
- `iceberg.silver.feature_platform_product_metadata` для mapping
  `product_id -> brand_id`.

Берутся только события `PRODUCT_VIEW`. В полном 28-дневном lookback строки S2b
дедуплицируются по `account_id,session_id,product_id`; `n_events` не суммируется.
После mapping в S1 каждое окно считается как сумма product-level distinct
session counts по товарам бренда. Поэтому одна сессия, в которой пользователь
открыл два разных товара одного бренда, даёт бренду два product-session
наблюдения.

```text
n_clicks_Nd = SUM(product-level distinct session_id)
```

S1 читается по точному дневному snapshot, соответствующему локальной дате
`calculated_at`. Click-окна полуоткрытые:
`[calculated_at - N days, calculated_at)`.

## GMV и ratios

Источники:

- `iceberg.silver.order_items`;
- `iceberg.silver.sku` для mapping `order_items.sku_id = sku.id -> product_id`;
- S1 для mapping `product_id -> brand_id`.

Учитываются позиции со статусом `COMPLETED`, `PAID`, `DELIVERED` или
`IN_DELIVERY`; B2B исключается условием `order_items.b2b_order = FALSE`.
Gold доверяет идентификаторам Silver и не повторяет входные range-фильтры.

```text
gmv_Nd = SUM(payment_price * item_quantity)
```

```text
gmv_Nd_ratio = gmv_Nd / total_account_gmv_Nd
```

`total_account_gmv_Nd` считается до фильтра `brand_id IS NOT NULL`: в
denominator остаются безбрендовые товары и товары без строки S1. Поэтому сумма
опубликованных brand ratios пользователя может быть меньше `1`. При нулевом
denominator ratio равна `NULL`. Фильтр `brand_id IS NOT NULL` применяется
только в финальном `SELECT`, после расчёта всех feature-значений.

## Запись и оркестрация

DAG запускается в `07:00` и `19:00 UTC`, то есть в `12:00` и `00:00
Asia/Tashkent. `start_date = 2026-09-05T07:00:00Z` — первый snapshot после
накопления полного 28-дневного окна Silver; `catchup=true`. Новый DAG создаётся
на паузе.

Перед Spark-задачей сенсоры ждут внутренние `dq`-таски S1 и S2b. Для дневного
S1 используется последний завершённый snapshot, соответствующий локальной дате
Gold. `order_items` и `sku` — внешние для репозитория источники; подтверждённые
upstream DQ DAG ids для них не заданы.

Повторный запуск выполняет `MERGE` по primary key и полностью синхронизирует
только текущий `calculated_at`. Таблица партиционирована по
`days(calculated_at)`. Используется общий Spark image с `git-sync` и
`resource_profile: small`.

## DQ, feature stats и alerts

DQ проверяет primary key, положительные IDs, отсутствие `brand_id = 160078`,
неотрицательные clicks/GMV и диапазон ratios `[0,1]`. Freshness и проверки
объёма на этапе отладки имеют severity `warn`.

Обязательная таска `feature_stats` профилирует числовые признаки записанного
snapshot. Alert-routing уровня P3 задан в конфиге для последующего включения,
но callback основного DAG, DQ и feature_stats временно отключены: G3 не
отправляет on-call уведомления во время отладки.

Потребители: Main и push. Ranking upload в этом контракте не настраивается.
