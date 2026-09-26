# product_cm2_main_features

Main CM2 рассчитывается на уровне SKU и публикуется после агрегации до товара.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_product_cm2_main_features`.
- DAG: `feature-platform.layers.gold.product_id.product_cm2_main_features`.
- Путь: `layers/gold/product_id/product_cm2_main_features/v1`.
- Grain и ключ: `calculated_at, product_id`.
- Расписание: `07:00` и `19:00 UTC`, то есть `12:00` и `00:00 Asia/Tashkent`.
- DAG создаётся paused; alert callbacks временно отключены на период отладки.
- Spark image — общий platform image с git-sync, resource profile — `small`.

DAG ждёт `dq` последнего применимого S6 snapshot. После записи параллельно запускаются
`dq` и `feature_stats`. `feature_stats` выполняет отдельный полный Trino-скан записанного
product snapshot. Downstream должен ждать только `dq`.

## Источники

- `iceberg.silver.feature_platform_sku_cm2_inputs_daily` — последний snapshot с
  `dt <= calculated_at`;
- `iceberg.gold.currency_rates` — последняя строка `currency_name = 'USD'` с
  `requested_dt <= calculated_at` и положительным rate.

## Расчёт

Глобальный p99.9 считается по непустым `sell_price_uzs` выбранного S6 snapshot. Цена каждого
SKU ограничивается этим значением до commission fallback и расчёта SKU score. Отсутствующая
комиссия заменяется на `20%`.

Константы совпадают с текущим Main producer:

- VAT divisor: `1.12`;
- seller compensation: `0.0028 × sell_price_uzs`;
- forward multiplier: `0.7`;
- logistics UZS: `SMALL=5000`, `MEDIUM=8000`, `LARGE=20000`;
- forward cost USD: `SMALL=0.3183`, `MEDIUM=1.9041`, `LARGE=6.3470`.

На SKU сначала считаются `net_inflow_usd` и `cm2_sku_usd`. Если суммарное
`n_orders_28d >= 5`, score товара является order-weighted средним SKU score; иначе берётся
простое среднее. Физическая feature-колонка:
`PRODUCT__score_uzs = score_usd × usd_rate`.

## Качество и публикация

DQ проверяет ключ, положительный `product_id`, заполненность и конечность CM2. Структурные
unit-тесты фиксируют один rate, порядок price cap/fallback и обе ветки агрегации. SKU не
публикуется. Ranking-service upload в этом изменении не добавляется; потребитель — Main.

`create_dbt_pr=false`, Iceberg maintenance sync остаётся включённым. После merge в master
нужно проверить автоматически созданный maintenance PR.
