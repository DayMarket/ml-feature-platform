# Product ranking features

DAG id: `feature-platform.layers.gold.product_id.product_ranking_features`.

Таблица: `iceberg.gold.feature_platform_product_ranking_features`.

Grain и primary key: `calculated_at,product_id`.

Namespace — `PRODUCT_RANKING`. Все физические feature-колонки содержат
этот префикс; `calculated_at` и `product_id` остаются без namespace.

## Назначение и population

G8 считает population-dependent ranks, percentiles, feedback smoothing и
return-rate baselines поверх полного G7 snapshot с тем же `calculated_at`.
Candidate, train, inference и A/B-фильтры не применяются. Каждая строка G7
сохраняется в G8.

G7 не публикует `l6_category_id`, поэтому G8 присоединяет L6 из
последнего S1 snapshot, не более нового, чем `calculated_at`. Это тот же
point-in-time mapping, который задаёт population G7. Отсутствующий L6 не
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

- price percentiles используют `PRODUCT_BASE__min_sell_price_eod`;
- order popularity использует `PRODUCT_BASE__product_orders_28d`;
- click popularity использует `PRODUCT_BASE__product_clicks_3d` и
  `PRODUCT_BASE__product_clicks_28d`;
- rating, discount и feedback quantity percentiles считаются внутри L6.

## Smoothed feedback rate

`alpha = 10`. Global prior считается на полном G7 snapshot:

```text
global_rate = sum(feedback_lte_3) / sum(product_orders_28d)

feedback_lte_3_to_orders_rate_smoothed =
    (feedback_lte_3 + alpha * global_rate)
    / (product_orders_28d + alpha)
```

Нулевой global denominator даёт `NULL`. Smoothed rate не ограничивается
единицей. Его average-rank percentile внутри L6 публикуется как
`feedback_lte_3_to_orders_rate_percentile_in_cat`.

## Return rates

G8 использует product-level counts из G7. В их population входят позиции
со статусами `COMPLETED`, `PAID`, `DELIVERED`, `IN_DELIVERY`, `RETURNED`;
`NOT_CREATED`, `CREATED` и остальные незавершённые статусы исключаются.
Количество возвратов определяется по `returned_quantity`, а не по одному
статусу строки, поэтому частично возвращённая позиция одновременно вносит
`item_quantity - returned_quantity` в `n_completed_Nd` и
`returned_quantity` в `n_returned_Nd`.

Для окон `7,14,28,60,90` дней category baseline считается как взвешенная
доля единиц, а не среднее product rates:

```text
l6_category_return_rate_Nd =
    sum(n_returned_Nd)
    / sum(n_completed_Nd + n_returned_Nd)
```

```text
return_rate_smoothed_Nd =
    (n_returned_Nd + 10 * l6_category_return_rate_Nd)
    / (n_completed_Nd + n_returned_Nd + 10)
```

Raw и smoothed rates делятся на L6 baseline для relative-признаков.
Если baseline отсутствует или равен нулю, relative rate равен `NULL`.
Канонические имена используют `smoothed`; вариант `smothed` не публикуется.

## Orchestration, DQ и feature stats

DAG работает в `07:00` и `19:00 UTC` (`12:00` и `00:00 Asia/Tashkent`),
ждёт DQ G7 с тем же logical date и DQ дневного S1 snapshot. `start_date` —
`2026-09-05T07:00:00Z`; `catchup=true`. DAG создаётся на паузе и использует
`resource_profile: small`.

Alert routing P3 настроен, но callbacks DAG, DQ и feature stats отключены на
время отладки. DQ проверяет ключи, percentile/rate ranges, неположительные
ranks и отсутствие NaN/infinity. `feature_stats` делает один Trino-скан
каждого snapshot по 33 числовым feature-колонкам.

Потребители: Main, push, train и G5. Group tag: `recsys-features`.
