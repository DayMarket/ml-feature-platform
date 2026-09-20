# Product ranking features

DAG id: `feature-platform.layers.gold.product_id.product_ranking_features`.

Таблица: `iceberg.gold.feature_platform_product_ranking_features`.

Grain и primary key: `calculated_at,product_id`.

Namespace — `PRODUCT`. Все физические feature-колонки содержат
этот префикс; `calculated_at` и `product_id` остаются без namespace.

## Назначение и population

G8 считает population-dependent ranks, percentiles, feedback smoothing и
return-rate baselines поверх полного G7 snapshot с тем же `calculated_at`.
Candidate, train, inference и A/B-фильтры не применяются. Каждая строка G7
сохраняется в G8.

G7 не публикует `category_id`, поэтому G8 присоединяет листовую категорию из
последнего S1 snapshot, не более нового, чем `calculated_at`. Это тот же
point-in-time mapping, который задаёт population G7. Отсутствующий `category_id` не
удаляет product из G8, но оставляет category-dependent признаки `NULL`.

## Rank и percentile

Все ties получают average rank:

```text
average_rank = rank + (tie_count - 1) / 2
percentile   = average_rank / N_non_null
negative_rank = -average_rank(metric DESC)
```

Наиболее популярный товар получает negative rank, ближайший к нулю.
Percentile denominator считает только non-NULL metric values. Global и category
ranks вычисляются на полной G7 population.

- price percentiles используют `PRODUCT__min_sell_price_eod`;
- order popularity использует `PRODUCT__orders_28d`;
- click popularity использует `PRODUCT__clicks_3d` и
  `PRODUCT__clicks_28d`;
- rating, discount и feedback quantity percentiles считаются внутри листовой категории.

## Smoothed feedback rate

`alpha = 10`. Global prior считается на полном G7 snapshot:

```text
global_rate = sum(feedback_lte_3_28d) / sum(orders_28d)

feedback_lte_3_to_orders_rate_smoothed =
    (feedback_lte_3_28d + alpha * global_rate)
    / (orders_28d + alpha)
```

Нулевой global denominator даёт `NULL`. Smoothed rate не ограничивается
единицей. Его average-rank percentile внутри листовой категории публикуется как
`feedback_lte_3_to_orders_rate_percentile_in_cat`.

## Return rates

G8 использует product-level counts из G7. В их population входят позиции
со статусами `COMPLETED`, `PAID`, `DELIVERED`, `IN_DELIVERY`, `RETURNED`;
`NOT_CREATED`, `CREATED` и остальные незавершённые статусы исключаются.
Каждая строка `order_items` имеет вес 1: статусы
`COMPLETED`, `PAID`, `DELIVERED`, `IN_DELIVERY` увеличивают `n_completed_Nd`,
а `RETURNED` увеличивает `n_returned_Nd`. Поля `item_quantity` и
`returned_quantity` для return-rate не используются.

Для окон `28,90` дней category baseline считается как взвешенная
доля строк, а не среднее product rates:

```text
category_return_rate_neg_Nd =
    -sum(n_returned_Nd)
    / sum(n_completed_Nd + n_returned_Nd)
```

```text
return_rate_neg_smoothed_Nd =
    -(n_returned_Nd - 10 * category_return_rate_neg_Nd)
    / (n_completed_Nd + n_returned_Nd + 10)
```

Raw и smoothed rates делятся на baseline листовой категории для relative-признаков.
Если baseline отсутствует или равен нулю, relative rate равен `NULL`.
Для сохранения направления «больше — лучше» relative-признаки используют
положительную product return rate, делённую на отрицательный category baseline:

```text
return_rate_to_category_return_neg_Nd =
    -return_rate_neg_Nd / category_return_rate_neg_Nd
```

Такие значения не превышают нуля: чем меньше возвратов у товара относительно
категории, тем ближе значение к нулю и тем лучше оно для score.
Канонические имена используют `smoothed`; вариант `smothed` не публикуется.

## Orchestration, DQ и feature stats

DAG работает в `07:00` и `19:00 UTC` (`12:00` и `00:00 Asia/Tashkent`),
ждёт DQ G7 с тем же logical date и DQ дневного S1 snapshot. `start_date` —
`2026-09-05T07:00:00Z`; `catchup=true`. DAG создаётся на паузе и использует
`resource_profile: small`.

Alert routing P3 настроен, но callbacks DAG, DQ и feature stats отключены на
время отладки. DQ проверяет ключи, percentile/rate ranges, неположительные
ranks и отсутствие NaN/infinity. `feature_stats` делает один Trino-скан
каждого snapshot по 21 числовой feature-колонке.

Потребители: Main, push, train и G5. Group tag: `recsys-features`.
