# Account-product features

DAG id: `feature-platform.layers.gold.account_id_product_id.account_product_features`.

Airflow group tag: `recsys-features`.

Alert уровня `P3` для команды `recsys` через `oncall_webhook_recsys` полностью
настроен, но callback основного DAG, DQ и feature_stats временно отключены на
период отладки.

Целевая таблица: `iceberg.gold.feature_platform_account_product_features`.

## Контракт

Путь сущности: `layers/gold/account_id_product_id/account_product_features/v1`.

Grain и primary key: `calculated_at,account_id,product_id`.

Идентификаторы и счётчики в физическом контракте имеют тип `INT`.

Namespace контракта — `ACCOUNT_PRODUCT`. Все физические feature-колонки уже
содержат его, например `ACCOUNT_PRODUCT__n_clicks_7d`; устаревший префикс
`pid_` не используется. Ключи `calculated_at`, `account_id` и `product_id`
остаются без namespace.

`calculated_at` — граница Gold snapshot: `00:00` или `12:00 Asia/Tashkent`.
Таблица содержит account-product пары, у которых есть хотя бы одно action-событие
за 28 дней или успешная покупка за 90 дней.

Семейства колонок:

- `n_clicks_{3,7,14,28}d` и `*_ratio`;
- `n_atcs_{3,7,14,28}d` и `*_ratio`;
- `n_atfs_{3,7,14,28}d` и `*_ratio`;
- `ACCOUNT_PRODUCT__neg_n_days_since_last_click` и
  `ACCOUNT_PRODUCT__neg_n_days_since_last_click_rel`;
- `n_orders_{3,7,14,28,60,90}d` и `*_ratio`;
- `ACCOUNT_PRODUCT__n_orders_28d_over_90d`;
- `gmv_{3,7,14,28,60,90}d` и `*_ratio`;
- `ACCOUNT_PRODUCT__neg_n_days_since_last_purchase`;
- `ACCOUNT_PRODUCT__neg_n_days_since_last_purchase_rel`;
- `ACCOUNT_PRODUCT__n_days_between_last_click_and_last_purchase`;
- `ACCOUNT_PRODUCT__last_click_before_last_purchase`.

## Action-события

Источник: `iceberg.silver.feature_platform_account_product_session_action_counts_12h`.

В каждом полном rolling-окне строки повторно дедуплицируются по
`account_id,session_id,product_id,event_type`. Поле `n_events` не суммируется.
Для фиксированных `account_id,product_id,event_type` считается
`COUNT(DISTINCT session_id)`:

- `PRODUCT_VIEW` формирует `n_clicks_*`;
- `ADD_TO_CART` формирует `n_atcs_*`;
- `ADD_TO_FAVORITES` формирует `n_atfs_*`.

Для recency берётся максимальный `last_received_at` события `PRODUCT_VIEW` за
28 дней:

```text
ACCOUNT_PRODUCT__neg_n_days_since_last_click =
    -(calculated_at - last_click_at) / 24 hours
```

Округление не применяется. Relative recency равна recency товара минус наиболее
свежее значение внутри того же `calculated_at,account_id`; самый свежий товар
получает `0`.

## Заказы и GMV

Источники:

- `iceberg.silver.order_items` — позиции заказов;
- `iceberg.silver.sku` — mapping `order_items.sku_id = sku.id` до `product_id`.

Учитываются позиции, созданные в полуинтервале
`[calculated_at - 90 days, calculated_at)` со статусом
`COMPLETED`, `PAID`, `DELIVERED` или `IN_DELIVERY`. B2B-позиции исключаются
условием `order_items.b2b_order = FALSE`.

Корректность идентификаторов `order_items` и `sku` считается гарантией Silver;
Gold не повторяет range-фильтры входных ID. Положительность ключей проверяется
после записи целевой партиции.

```text
n_orders_Nd = COUNT(DISTINCT order_id)
gmv_Nd = SUM(payment_price * item_quantity)
```

`ACCOUNT_PRODUCT__n_orders_28d_over_90d` равна `ACCOUNT_PRODUCT__n_orders_28d / ACCOUNT_PRODUCT__n_orders_90d` и
остаётся `NULL` при нулевом denominator.

Purchase recency использует последний `generated_at` успешной позиции за 90 дней:

```text
ACCOUNT_PRODUCT__neg_n_days_since_last_purchase =
    -(calculated_at - last_purchase_at) / 24 hours
```

Результат остаётся дробным числом дней; округление не применяется.

Relative purchase-recency показывает, насколько покупка конкретного товара
старее самой свежей покупки пользователя:

```text
ACCOUNT_PRODUCT__neg_n_days_since_last_purchase_rel =
    ACCOUNT_PRODUCT__neg_n_days_since_last_purchase
    - MAX(ACCOUNT_PRODUCT__neg_n_days_since_last_purchase) OVER (account_id)
```

Самый недавно купленный товар получает `0`, более старые — отрицательные
значения. Для товаров без покупки результат остаётся `NULL`; `COALESCE` не
применяется.

Знаковый интервал между последними click и purchase:

```text
ACCOUNT_PRODUCT__n_days_between_last_click_and_last_purchase =
    n_days_since_last_click - n_days_since_last_purchase
```

В терминах публикуемых отрицательных recency это
`ACCOUNT_PRODUCT__neg_n_days_since_last_purchase - ACCOUNT_PRODUCT__neg_n_days_since_last_click`. Отрицательное
значение означает, что click произошёл позднее purchase; положительное — что
purchase произошла позднее click. `COALESCE` не применяется: если одного из
timestamps нет, результат равен `NULL`.

`ACCOUNT_PRODUCT__last_click_before_last_purchase = 1`, если последний click за 28 дней произошёл
позднее последней purchase за 90 дней. Несмотря на legacy-название, сравнение
выполняется именно так и напрямую по UTC timestamps. Если одного из timestamps
нет, результат равен `NULL`.

## Ratios и NULL

Для каждого исходного count или GMV:

```text
ratio = product_value / SUM(product_value)
        OVER (PARTITION BY calculated_at, account_id)
```

При нулевом denominator ratio равна `NULL`. Если пара попала в таблицу только из
одного источника, отсутствующие action/order counts и GMV заполняются нулями;
recency без соответствующего события остаётся `NULL`.

## Запись и оркестрация

DAG запускается в `07:00` и `19:00 UTC`, то есть в `12:00` и `00:00
Asia/Tashkent`. `start_date = 2026-09-05T07:00:00Z` — первый snapshot после
накопления полного 28-дневного окна S2c; `catchup=true`.

Перед Spark-задачей `ExternalTaskSensor` ждёт внутреннюю задачу `dq` S2c с тем
же logical date. `order_items` и `sku` являются внешними источниками платформы;
подтверждённые upstream DQ DAG ids для них не заданы.

Повторный запуск выполняет `MERGE` по primary key и полностью синхронизирует
только текущий `calculated_at`. Таблица партиционирована по
`days(calculated_at)`.

Используется общий Spark image с `git-sync` и `resource_profile: small`.

## DQ и потребители

DQ проверяет primary key, положительные ID, неотрицательные counts/GMV, диапазон
ratios `[0,1]`, монотонность `orders_28d <= orders_90d`, неположительную recency,
максимум `ACCOUNT_PRODUCT__neg_n_days_since_last_click_rel = 0` для каждого account с
кликами и домен legacy-флага. Freshness и проверки объёма при первой раскатке
имеют severity `warn`. Alert callbacks DQ и feature_stats остаются отключёнными
до окончания отладки.

Потребители: Main, push, train и account-candidate joins. Ranking upload в этом
контракте пока не настраивается.
