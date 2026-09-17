# product_cm2_pdp_features

Независимый PDP CM2 рассчитывается на уровне SKU и агрегируется до товара. Формула не
объединяется с Main CM2.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_product_cm2_pdp_features`.
- DAG: `feature-platform.layers.gold.product_id.product_cm2_pdp_features`.
- Путь: `layers/gold/product_id/product_cm2_pdp_features/v1`.
- Grain и ключ: `calculated_at, product_id`.
- Расписание: `07:00` и `19:00 UTC`, то есть `12:00` и `00:00 Asia/Tashkent`.
- DAG создаётся paused; alert callbacks временно отключены на период отладки.
- Spark image — общий platform image с git-sync, resource profile — `small`.

DAG ждёт `dq` последнего применимого S6 snapshot. После записи параллельно запускаются
`dq` и `feature_stats`. `feature_stats` выполняет отдельный полный Trino-скан записанного
product snapshot. Downstream должен ждать только `dq`.

## Источники и фильтры

- `iceberg.silver.feature_platform_sku_cm2_inputs_daily` — последний snapshot с
  `dt <= calculated_at`;
- `iceberg.gold.currency_rates` — последняя строка `currency_name = 'USD'` с
  `requested_dt <= calculated_at` и положительным rate.

Глобальный p99.9 считается по непустым `sell_price_uzs` всего выбранного S6 snapshot. Цена
ограничивается p99.9 до исключения SKU с `commission_pct IS NULL`. SKU без комиссии не
участвуют в PDP-агрегации.

## Расчёт

Для SKU:

`net_inflow_sku = capped_sell_price_uzs × commission_pct / 100 / 1.12 / usd_rate`.

Logistics, forward cost и seller compensation не вычитаются. Если сумма `n_orders_28d`
товара не меньше 5, `net_inflow` и capped sell price агрегируются с весом `n_orders_28d`;
иначе используется простое среднее по SKU.

Физические feature-колонки:

- `PRODUCT_CM2_PDP__cm2` — равен `PRODUCT_CM2_PDP__net_inflow`;
- `PRODUCT_CM2_PDP__net_inflow` — агрегированный net inflow в USD;
- `PRODUCT_CM2_PDP__weighted_price` — агрегированная capped sell price в UZS;
- `PRODUCT_CM2_PDP__today_rate` — использованный USD rate.

Legacy alias `price` не публикуется.

## Качество и публикация

DQ проверяет ключ, положительный `product_id`, конечность значений, `cm2 = net_inflow`,
неотрицательную цену и положительный rate. Структурные unit-тесты фиксируют price cap до
commission filter, отсутствие commission fallback и обе ветки агрегации. SKU не публикуется.
Ranking-service upload не добавляется; потребители — PDP и CPO.

`create_dbt_pr=false`, Iceberg maintenance sync остаётся включённым. После merge в master
нужно проверить автоматически созданный maintenance PR.
