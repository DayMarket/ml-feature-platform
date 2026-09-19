# Account-profile features

DAG id: `feature-platform.layers.gold.account_id.account_profile_features`.

Таблица: `iceberg.gold.feature_platform_account_profile_features`.

Grain и primary key: `calculated_at,account_id`.

Namespace — `ACCOUNT`. Все физические feature-колонки уже содержат его,
например `ACCOUNT__last_clicked_avg_price`. Ключи `calculated_at` и
`account_id` остаются без namespace.

## Population

Snapshot содержит объединение account IDs из:

- S5 demographics;
- пользователей с успешными заказами за 90 дней;
- пользователей с `PRODUCT_VIEW` за 28 дней, у которых после отбора последних
  75 product-session наблюдений остался хотя бы один товар с ценой.

Если account появился только из одного источника, отсутствующие блоки остаются
`NULL`.

## Demographics

S5 читается по началу локальной даты snapshot. Публикуются `ACCOUNT__gender`, бинарный
`ACCOUNT__gender_is_female`, `ACCOUNT__age`, `ACCOUNT__age_bucket`, `ACCOUNT__city_name` и `ACCOUNT__platform`.

Age buckets: `LT_18`, `18_24`, `25_34`, `35_44`, `45_54`, `55_PLUS`,
`UNKNOWN`.

## Order profile

Используются окна 7, 28 и 90 дней. Учитываются статусы `COMPLETED`, `PAID`,
`DELIVERED`, `IN_DELIVERY`; B2B-позиции исключаются. SKU преобразуется в
`product_id` через `iceberg.silver.sku`.

SKU mapping присоединяется через `LEFT JOIN`: отсутствие текущего mapping не
удаляет строку из account-level order total и item-price расчётов, но оставляет
недоступными product-dependent признаки этой строки.

Сначала рассчитывается total заказа:

```text
line_gmv = payment_price * item_quantity
order_total = SUM(line_gmv) GROUP BY account_id, order_id
```

Median, min, max и sum затем считаются по заказам. Средняя цена позиции равна
общему GMV, делённому на общее `item_quantity`.

Discount, rating, popularity и ACCOUNT__gender категории взвешиваются строками
`order_items`: строки не дедуплицируются до одного product, но `item_quantity`
не создаёт дополнительный вес для этих семейств.

Product attributes присоединяются point-in-time на snapshot `T`:

- discount и rating — из физических колонок G7
  `PRODUCT__discount` и `PRODUCT__rating` с
  `calculated_at = T`;
- global и leaf-category popularity rank — из физических колонок G8
  `PRODUCT_STATS__popularity_by_orders_neg_rank` и
  `PRODUCT_STATS__popularity_by_orders_neg_rank_in_cat` с
  `calculated_at = T`;
- листовая `category_id` — из S1 snapshot текущей локальной даты;
- population male/female shares и gender листовой категории — из G6 с
  `calculated_at = T`. Purchased male/female category shares — это среднее
  `male_product_session_share_28d` / `female_product_session_share_28d` по
  строкам заказов в соответствующем окне. Unisex share остаётся долей строк с
  category gender `U` или `NULL`.

`last_purchased_neg_p90_popularity_rank_*` вычисляется как p10 уже
отрицательного rank, то есть как `-p90` положительного rank. Симметричная
граница `last_purchased_neg_p10_popularity_rank_*` вычисляется как p90
отрицательного rank.

## Last-clicked profile

1. S2c читается на полуоткрытом окне `[T - 28 days, T)`.
2. `PRODUCT_VIEW` дедуплицируется по
   `account_id,session_id,product_id` с `MAX(last_received_at)`.
3. На account остаются последние 75 строк по `last_received_at`.
4. Присоединяются текущие S3 price, S1 leaf category, G6 category demographics, G7 rating и
   G8 order popularity rank.
5. Строки без `min_sell_price_eod` удаляются.

Price, category population shares, rating и popularity агрегируются по оставшимся
product-session наблюдениям. `*_male_cat_share_raw` и
`*_female_cat_share_raw` — средние G6 product-session shares по категориям
последних кликов; они не являются долями category labels. Нормализованные
shares делят эти две величины на их сумму.

Price percentiles рассчитываются на полной account population snapshot с average-rank tie
semantics:

```text
percentile = average_rank / N_non_null
```

## Snapshot time semantics

Gold `calculated_at`, S2c и будущие G6–G8 используют локальный clock-value
`00:00`/`12:00 Asia/Tashkent`. S3 и S5 хранят начало локального дня как
`00:00` clock-value. S1 физически хранит тот же локальный день как UTC-момент
`19:00` предыдущей даты; запрос учитывает это различие явно.

## Orchestration and dependencies

DAG работает в `07:00` и `19:00 UTC`, то есть `12:00` и `00:00`
Asia/Tashkent. Он ждёт DQ-таски S1, S2c, S3, S5, G6, G7 и G8. G6–G8 должны
быть опубликованы до включения G5. `start_date = 2026-09-05T07:00:00Z`,
`catchup=true`; DAG создаётся на паузе.

Spark запускается с `resource_profile: small`. Alert routing P3 настроен, но
callbacks DAG, DQ и feature stats отключены на время отладки.

## DQ and feature stats

DQ проверяет primary key, положительный `account_id`, домены demographics,
неотрицательные order totals и prices, порядок min/median/max, а также диапазоны
shares, ratings, discounts и percentiles. SQL unit tests фиксируют ограничение
75 last-clicked строк и point-in-time joins.

`feature_stats` выполняет отдельный Trino-скан каждого 12-часового snapshot.
Ranking upload не настраивается. Потребители: Main, push и train.

После merge в `master` dbt PR не создаётся (`create_dbt_pr: false`), а CI может
создать maintenance PR в `DayMarket/pyspark-etl`
(`create_maintenance_pr: true`).
