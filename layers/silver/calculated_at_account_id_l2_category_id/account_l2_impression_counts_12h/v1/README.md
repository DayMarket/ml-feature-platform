# 12-часовые показы account/L2

DAG id: `feature-platform.layers.silver.calculated_at_account_id_l2_category_id.account_l2_impression_counts_12h`.

Airflow group tag: `recsys-main-page-features`.

Целевая таблица: `iceberg.silver.feature_platform_account_l2_impression_counts_12h`.

## Контракт

Grain и primary key: `calculated_at,account_id,l2_category_id`.

- `calculated_at` — правая граница 12-часового интервала в UTC;
- `account_id` — положительный ID пользователя;
- `l2_category_id` — второй содержательный уровень либо L1, если отдельного L2 нет;
- `n_impressions` — сумма session-level distinct product impressions.

Сырые product-level impression-события в таблице не сохраняются.

## Источники и категории

Источники:

- `iceberg.silver_b2c_clickstream.events`;
- `iceberg.silver.product`;
- `iceberg.silver_apidb_kazanexpress.public_category`.

Для события сначала используется `event.category_id`, если этот ID разрешается в содержательный категорийный путь. Иначе применяется fallback `event.product_id -> product.category_id`. После этого конечная категория приводится к L2. Если в пути нет отдельного L2, используется L1.

Технический root `category_id = 1` исключается. Поддерживается до десяти содержательных уровней; при более глубокой иерархии job завершается ошибкой до записи.

## Окно и distinct-семантика

Для каждого `calculated_at` читается полуинтервал:

```text
[calculated_at - 12 hours, calculated_at)
```

Фильтры:

- `event_type = 'PRODUCT_IMPRESSION'`;
- `account_id > 0`;
- `product_id > 0`;
- `session_id IS NOT NULL`;
- фильтра по `space` нет.

Сначала считается `COUNT(DISTINCT product_id)` внутри `account_id,session_id,l2_category_id`. Затем session-level значения суммируются до `account_id,l2_category_id`.

Один `session_id,product_id`, попавший в разные 12-часовые срезы, учитывается в каждом срезе отдельно.

## Оркестрация и хранение

DAG запускается в `07:00` и `19:00 UTC`, то есть в `12:00` и `00:00 Asia/Tashkent`. `calculated_at` берётся из UTC `data_interval_end`.

`start_date = 2026-07-05 19:00 UTC` открывает полный 31-дневный диапазон перед первым плановым запуском 5 августа. `catchup=false`, поэтому включение нового DAG не запускает исторический расчёт автоматически. Перед подключением 30-дневных Gold-окон нужен отдельный контролируемый backfill этого диапазона.

Iceberg использует `hours(calculated_at)`. Это сохраняет два независимых среза и позволяет идемпотентно перезаписать один `calculated_at` через `overwritePartitions()`.

Подтверждённые upstream DQ DAG ids для внешних таблиц-источников неизвестны, поэтому отдельные sensors не добавлены.

CI создаёт стандартные dbt DQ-тесты: уникальность primary key и `not_null` для всех его колонок. `n_impressions > 0`, временные границы и положительные ID обеспечиваются самой трансформацией.

Активного model consumer на момент проектирования нет. Контракт сохраняется для согласованности L1/L2 и будущего использования; ranking upload не настраивается.
