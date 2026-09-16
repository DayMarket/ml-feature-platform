# Product base features

DAG id: `feature-platform.layers.gold.product_id.product_base_features`.

Таблица: `iceberg.gold.feature_platform_product_base_features`.

Grain и primary key: `calculated_at,product_id`.

Namespace — `PRODUCT_BASE`. Все физические feature-колонки содержат этот
префикс; ключи `calculated_at` и `product_id` остаются без namespace.

## Назначение и population

G7 хранит базовые product-level признаки без population-dependent ranks,
percentiles, category baselines и smoothing. G8 читает полный G7 snapshot и
считает зависящие от population признаки отдельно.

Population — все товары последнего S1 snapshot не позднее `calculated_at`.
Цены, actions, orders, feedback, returns и G6 присоединяются через `LEFT JOIN`.
Отсутствующие аддитивные counts становятся нулями; отсутствующие цены, rating,
rates и category attributes остаются `NULL`.

## Источники

- S1 `iceberg.silver.feature_platform_product_metadata` — population, L6 и
  `created_at`;
- S2c `iceberg.silver.feature_platform_account_product_session_action_counts_12h`;
- S3 `iceberg.silver.feature_platform_product_prices_daily`;
- S4 `iceberg.silver.feature_platform_product_feedback_counts_12h`;
- `iceberg.silver.order_items` и `iceberg.silver.sku`;
- `iceberg.gold.feature_platform_product_feedback_base_stats`;
- G6 `iceberg.gold.feature_platform_l6_category_gender_features`.

S1 и S3 читаются по последнему snapshot, который не новее расчёта. S2c/S4 и
G6 ограничиваются текущим `calculated_at`. All-time feedback берётся из
последней доступной дневной партиции, не более поздней, чем UTC-дата расчёта.

## Price и content

Из S3 публикуются min/avg/max sell prices, min/max full prices и min/avg/max
sell prices доступных SKU. `minimal_sell_price` и `minimal_full_price` —
compatibility-копии соответствующих min-колонок.

```text
product_discount = clip(100 * (1 - min_sell_price_eod / min_full_price_eod), 0, 100)
```

При отсутствующей или неположительной `min_full_price_eod` discount равен
нулю. Если sell price отсутствует при положительной full price, discount
остаётся `NULL`.

`age_in_days` — число локальных календарных дней между `S1.created_at` и
`calculated_at` в `Asia/Tashkent`; отсутствующая или будущая дата даёт `NULL`.

## Product actions

Product-level actions сохраняют event multiplicity и суммируют `S2c.n_events`:

- `product_clicks_{3,28}d` — `PRODUCT_VIEW`;
- `favorites_daily` и `favorites_last_{3,7,14,21,28}d` —
  `ADD_TO_FAVORITES`.

В отличие от account-level Gold, session distinct здесь не применяется.
Product-level impressions намеренно отсутствуют.

## Orders и category shares

Успешные order features используют статусы `COMPLETED`, `PAID`, `DELIVERED`,
`IN_DELIVERY`; B2B исключаются. `order_items.sku_id` маппится в `product_id`
через `silver.sku`.

- `orders_quantity_daily` и `product_orders_{7,28,90}d` — distinct `order_id`;
- `items_purchased_quantity_daily` — сумма `item_quantity`;
- `product_orders_total` — distinct успешных заказов за всю историю;
- `has_product_orders_total` — бинарный флаг.

Category denominator считается как сумма product-level order counts:

```text
l6_category_orders_Nd = sum(product_orders_Nd) over l6_category_id
product_orders_share_in_l6_category_Nd = product_orders_Nd / l6_category_orders_Nd
```

Заказ с двумя разными товарами одной L6 участвует в denominator дважды — по
одному product occurrence на каждый товар.

## Feedback

S4 даёт rolling признаки за `3,7,14,21,28` дней: количество feedback, сумму
rating, counts `rating >= 4`/`rating <= 3`, их доли и средний rating. Daily
counts используют последние 24 часа.

All-time признаки пересчитываются из пяти rating bucket существующего feedback
Gold: `product_rating`, `product_feedback_quantity`, `feedback_gte_4`,
`feedback_lte_3`, соответствующие ratios и `log_feedback_quantity`.

Raw feedback-to-orders rates делят all-time feedback counts на
`product_orders_total`. `feedback_lte_3_to_orders_rate` использует в
знаменателе `product_orders_28d`. Нулевой denominator даёт `NULL`; rates не
ограничиваются единицей.

Population-dependent `feedback_lte_3_to_orders_rate_smoothed` в G7 не
публикуется: global prior, category baseline и percentile принадлежат G8.

## Returns

Для окон `7,14,28,60,90` дней B2B исключаются, но successful status set не
применяется:

```text
n_completed_Nd = sum(item_quantity - coalesce(returned_quantity, 0))
n_returned_Nd  = sum(coalesce(returned_quantity, 0))
return_rate_Nd = n_returned_Nd / (n_completed_Nd + n_returned_Nd)
```

Статус `RETURNED` не трактуется как полный возврат: используется только
`returned_quantity` конкретной позиции.

## Category gender

G6 присоединяется по `S1.l6_category_id` и переносит unique clickers, weighted
female/male product-session shares и category gender flags. Legacy-колонка
`n_unique_cat_clicks_by_women_28d` равна weighted female share, а не unique
count. При отсутствии строки G6 все category gender-признаки остаются `NULL`.

## Orchestration, DQ и feature stats

DAG работает в `07:00` и `19:00 UTC` (`12:00` и `00:00 Asia/Tashkent`), ждёт
DQ S1/S2c/S3/S4, all-time feedback Gold и G6. `start_date` —
`2026-09-05T07:00:00Z`, первый согласованный Gold snapshot после накопления 28
дней S2c/S4. DAG создаётся на паузе; Spark использует `resource_profile: small`.

Alert routing P3 настроен, но callbacks DAG, DQ и feature stats отключены на
время отладки. DQ проверяет ключи, ranges, count-инварианты и монотонность окон.
`feature_stats` выполняет отдельный Trino-скан каждого 12-часового snapshot для
всех числовых feature-колонок. Ranking upload в этой задаче не настраивается.

Потребители: Main, push, train, G5 и G8. Group tag: `recsys-features`.
