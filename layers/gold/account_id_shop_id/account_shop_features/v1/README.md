# Account-shop features

DAG id: `feature-platform.layers.gold.account_id_shop_id.account_shop_features`.

Airflow group tag: `recsys-features`.

## Output

Таблица: `iceberg.gold.feature_platform_account_shop_features`.

Путь: `layers/gold/account_id_shop_id/account_shop_features/v1`.

Grain и primary key: `calculated_at,account_id,shop_id`.

Namespace — `ACCOUNT_SHOP`. Все физические feature-колонки уже содержат его,
например `ACCOUNT_SHOP__n_clicks_7d`; устаревший префикс `sid_` не используется.
Ключи `calculated_at`, `account_id` и `shop_id` остаются без namespace.

`calculated_at` — граница Gold snapshot: `00:00` или `12:00 Asia/Tashkent`.
Строки без `shop_id` не публикуются.

## Sources

- `iceberg.silver.feature_platform_product_metadata` (S1):
  `product_id -> shop_id`;
- `iceberg.silver.feature_platform_account_product_session_action_counts_12h`
  (S2c): account-product actions;
- `iceberg.silver.order_items`: позиции заказов;
- `iceberg.silver.sku`: `order_items.sku_id -> sku.id -> product_id`.

S1 и S2c принадлежат этому репозиторию, поэтому DAG ждёт их `dq`-таски.
Для `order_items` и `sku` подтверждённые upstream DQ DAG ids не заданы.

S1 читается по snapshot на начало локальной даты `calculated_at`. В SQL эта
граница переводится в UTC: например, `2026-09-10 00:00 Asia/Tashkent` читается
как `2026-09-09 19:00 UTC`. Один mapping используется для actions и заказов
текущего Gold snapshot.

## Actions

Сигналы:

- `PRODUCT_VIEW -> clicks`;
- `ADD_TO_CART -> atcs`;
- `ADD_TO_FAVORITES -> atfs`.

На полном 28-дневном lookback S2c дедуплицируется по
`account_id,session_id,product_id,event_type`. Берётся максимальный
`last_received_at`; `n_events` не суммируется. Одна сессия с двумя разными
товарами одного магазина даёт два product-session наблюдения.

Для каждого сигнала публикуются counts и ratios за 3, 7, 14 и 28 дней:

```text
n_{signal}_Nd = COUNT(product-session observations)
```

## Orders and GMV

Учитываются позиции со статусами `COMPLETED`, `PAID`, `DELIVERED` и
`IN_DELIVERY`. Строки `b2b_order = TRUE` исключаются.

Для окон 3, 7, 14, 28, 60 и 90 дней:

```text
n_orders_Nd = COUNT(DISTINCT order_id) GROUP BY account_id, shop_id
gmv_Nd = SUM(payment_price * item_quantity) GROUP BY account_id, shop_id
```

Несколько товаров одного магазина в заказе дают один shop-order occurrence.
Один заказ с товарами двух магазинов учитывается по одному разу в каждом
магазине. Все строки заказа сохраняют вклад в GMV.

## Ratios and NULLs

Для каждой count/GMV-колонки:

```text
shop_ratio = shop_value
             / SUM(shop_value) OVER (PARTITION BY calculated_at, account_id)
```

Поэтому мульти-магазинный заказ входит в denominator `n_orders` по одному разу
для каждого магазина. При нулевом denominator ratio равна `NULL`.

Если ключ появился только из другого семейства, отсутствующие counts и GMV
заполняются нулём. Фильтр `brand_id = 160078` не используется.

Все окна полуоткрытые: `[calculated_at - N days, calculated_at)`.

## Orchestration

DAG работает в `07:00` и `19:00 UTC`, то есть в `12:00` и `00:00`
Asia/Tashkent. `start_date = 2026-09-05T07:00:00Z`, `catchup=true`; DAG создаётся
на паузе. Дата выбрана как первый snapshot с полным 28-дневным action-lookback
после старта S2c 8 августа 2026 года.

Spark использует общий image с git-sync и `resource_profile: small`. Повторный
запуск синхронизирует через `MERGE` только текущий `calculated_at`.

## DQ, feature stats and alerts

DQ проверяет primary key, положительные IDs, неотрицательные значения,
монотонность окон и диапазон ratios `[0,1]`. SQL unit-тест фиксирует distinct
`order_id` на shop-grain и denominator ratios. Отдельный production DQ со
сканированием источника для parity заказа не добавляется.

`feature_stats` профилирует каждый новый 12-часовой snapshot отдельным Trino
сканом. Alert routing P3 задан, но callbacks основного DAG, DQ и feature_stats
отключены до завершения отладки.

Ranking upload не настраивается. Потребители: Main и push.

После merge в `master` dbt PR не создаётся (`create_dbt_pr: false`), а CI может
создать maintenance PR в `DayMarket/pyspark-etl`
(`create_maintenance_pr: true`).
