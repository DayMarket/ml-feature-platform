# Gold Price Features по SKU Group ID

DAG id: `feature-platform.layers.gold.sku_group_id.sku_group_price_features`.

Пайплайн собирает дневные ценовые признаки на уровне `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_sku_group_price_features`.

Источник: `iceberg.silver.feature_platform_sku_group_id_prices`.

Основная логика:

- за дату расчета `ds` берет агрегаты цен из silver-таблицы;
- джойнится с `iceberg.silver.sku`, чтобы получить `category_id`;
- считает среднюю цену продажи внутри категории;
- записывает `sell_price_eod` как `log1p(avg_sell_price_eod)`;
- считает абсолютную скидку `median_full_price_eod - median_sell_price_eod`;
- считает долю цены продажи от полной цены `median_sell_price_eod / median_full_price_eod`;
- для вчерашнего дня относительно `ds` считает отношение текущей минимальной полной цены к средней минимальной полной цене за предыдущие 14 и 30 дней.

Партиция результата соответствует Airflow `ds`.

Перед расчетом job создает Iceberg-таблицу из `migrations/create_table.sql`, если она еще не существует.

## DQ

DQ-тесты выполняются таской `dq` внутри этого DAG'а сразу после записи партиции;
каталог тестов и правила конфигурирования описаны в `dq/README.md`.

Базовый набор: `primary_key_not_null`, `primary_key_unique`, `row_count_min`,
`row_count_growth`, `freshness`.

Пороги подобраны по истории таблицы в Trino (81 партиция с 2026-06-01):

- `row_count_min: 4000000` — минимум за последние 30 партиций 5.24M строк;
- `row_count_growth: 0.02` — по 78 парам соседних партиций рост лежит в
  +0.11%..+0.46% и ни разу не был отрицательным.

`not_null` покрывает четыре колонки, которые считаются из цен текущего дня.
`ratio_crnt_min_to_avg_min_full_price_14/30` опираются на историю 14 и 30 дней, поэтому
у молодых `sku_group_id` их нет: наблюдаемая доля NULL — 0.31%. Вместо жёсткого
`not_null` на них стоит `null_share_below` с `max_share: 0.05` — это 16x к норме,
тест срабатывает только когда история цен реально развалилась.

`non_negative` покрывает все 6 фичевых колонок.

Сенсор на silver-витрину цен переведён с `dbt.source.trino.*.dq` на таску `dq` DAG'а
`feature-platform.layers.silver.sku_group_id.sku_group_id_prices`; расписания
`01:00` против `02:00` дают прежнюю разницу в час.

Аплоад ranking-фич ждёт таску `dq` этого DAG'а, а не весь DAG и не dbt-DQ-DAG.
