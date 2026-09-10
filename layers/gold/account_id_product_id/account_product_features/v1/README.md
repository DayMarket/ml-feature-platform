# Account-product features

DAG id: `feature-platform.layers.gold.account_id_product_id.account_product_features`.

Airflow group tag: `recsys-features`.

Целевая таблица: `iceberg.gold.feature_platform_account_product_features`.

## Контракт

Путь сущности: `layers/gold/account_id_product_id/account_product_features/v1`.

Grain и primary key: `calculated_at,account_id,product_id`.

`calculated_at` — граница Gold snapshot: `00:00` или `12:00 Asia/Tashkent`.
Таблица содержит account-product пары, у которых есть хотя бы одно action-событие
за 28 дней или успешная покупка за 90 дней.

Семейства колонок:

- `pid_n_clicks_{3,7,14,28}d` и `*_ratio`;
- `pid_n_atcs_{3,7,14,28}d` и `*_ratio`;
- `pid_n_atfs_{3,7,14,28}d` и `*_ratio`;
- `pid_neg_n_hours_since_last_click` и
  `pid_neg_n_hours_since_last_click_rel`;
- `pid_n_orders_{3,7,14,28,60,90}d` и `*_ratio`;
- `pid_n_orders_28d_over_90d`;
- `pid_gmv_{3,7,14,28,60,90}d` и `*_ratio`;
- `pid_neg_n_days_since_last_purchase`;
- `last_click_before_last_purchase`.

Устаревшие `pid_neg_n_days_since_last_click*` не публикуются: click-recency
хранится в точных дробных часах.

## Action-события

Источник: `iceberg.silver.feature_platform_account_product_session_action_counts_12h`.

В каждом полном rolling-окне строки повторно дедуплицируются по
`account_id,session_id,product_id,event_type`. Поле `n_events` не суммируется.
Для фиксированных `account_id,product_id,event_type` считается
`COUNT(DISTINCT session_id)`:

- `PRODUCT_VIEW` формирует `pid_n_clicks_*`;
- `ADD_TO_CART` формирует `pid_n_atcs_*`;
- `ADD_TO_FAVORITES` формирует `pid_n_atfs_*`.

Для recency берётся максимальный `last_received_at` события `PRODUCT_VIEW` за
28 дней:

```text
pid_neg_n_hours_since_last_click =
    -(calculated_at - last_click_at) / 1 hour
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
`COMPLETED`, `PAID`, `DELIVERED` или `IN_DELIVERY`. Требуются положительные
`account_id`, `order_id` и `product_id` в signed 32-bit диапазоне.

```text
pid_n_orders_Nd = COUNT(DISTINCT order_id)
pid_gmv_Nd = SUM(payment_price * item_quantity)
```

`pid_n_orders_28d_over_90d` равна `pid_n_orders_28d / pid_n_orders_90d` и
остаётся `NULL` при нулевом denominator.

Purchase recency использует последний `generated_at` успешной позиции за 90 дней:

```text
pid_neg_n_days_since_last_purchase =
    -CEIL((calculated_at - last_purchase_at) / 24 hours)
```

`last_click_before_last_purchase = 1`, если последний click за 28 дней произошёл
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
Asia/Tashkent`. `start_date = 2026-08-08T07:00:00Z`, `catchup=true`.

Перед Spark-задачей `ExternalTaskSensor` ждёт внутреннюю задачу `dq` S2c с тем
же logical date. `order_items` и `sku` являются внешними источниками платформы;
подтверждённые upstream DQ DAG ids для них не заданы.

Повторный запуск выполняет `MERGE` по primary key и полностью синхронизирует
только текущий `calculated_at`. Таблица партиционирована по
`days(calculated_at)`.

Используется общий Spark image с `git-sync` и `resource_profile: small`.

## DQ и потребители

DQ проверяет primary key, положительные ID, неотрицательные counts/GMV, диапазон
ratios `[0,1]`, монотонность `orders_28d <= orders_90d`, неположительную recency
и домен legacy-флага. Freshness и проверки объёма при первой раскатке имеют
severity `warn`.

Потребители: Main, push, train и account-candidate joins. Ranking upload в этом
контракте пока не настраивается.
