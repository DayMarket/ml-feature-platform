# 12-часовые feedback-факты товара

DAG id: `feature-platform.layers.silver.calculated_at_product_id.product_feedback_counts_12h`.

Airflow group tag: `recsys-main-page-features`.

Целевая таблица: `iceberg.silver.feature_platform_product_feedback_counts_12h`.

## Контракт

Путь сущности:
`layers/silver/calculated_at_product_id/product_feedback_counts_12h/v1`.

Grain и primary key: `calculated_at,product_id`.

- `calculated_at` — правая граница 12-часового интервала в UTC;
- `product_id` — положительный ID товара;
- `feedback_count` — количество опубликованных отзывов с валидным рейтингом;
- `rating_sum` — сумма валидных рейтингов;
- `feedback_gte_4` — количество рейтингов 4–5;
- `feedback_lte_3` — количество рейтингов 1–3.

Отдельный `rated_feedback_count` не хранится, поскольку он равен `feedback_count`.
SKU, SKU-group, средний рейтинг и all-time показатели в S4 не публикуются.

## Источник и окно

Источник: `iceberg.silver_bxappdb2_foodback.public_feedback`.

Timestamp события — `date_published`. Spark session работает в UTC, и срез использует
полуоткрытый интервал:

```text
[calculated_at - 12 hours, calculated_at)
```

Фильтры:

- `status = 'PUBLISHED'`;
- `product_id > 0`;
- `date_published >= calculated_at - 12 hours`;
- `date_published < calculated_at`.

Контракт предполагает, что опубликованный отзыв не возвращается в `UNPUBLISHED`, не
публикуется повторно и физически не удаляется. Изменения рейтинга после публикации намеренно
не пересчитывают старые 12-часовые срезы.

## Расчёт и rolling-семантика

Для каждого `product_id` считаются:

```text
feedback_count = COUNT(rating)
rating_sum = SUM(rating)
feedback_gte_4 = COUNT(rating >= 4)
feedback_lte_3 = COUNT(rating <= 3)
```

Все четыре показателя аддитивны. Rolling-окна в Gold строятся их суммированием. Средний рейтинг
за окно при необходимости рассчитывается как `SUM(rating_sum) / SUM(feedback_count)` с
проверкой ненулевого знаменателя.

All-time показатели не рассчитываются через S4. Для них сохраняется существующий контракт
`iceberg.gold.feature_platform_product_feedback_base_stats`.

## Оркестрация и хранение

DAG запускается в `07:00` и `19:00 UTC`, синхронно с другими 12-часовыми Silver-срезами группы
`recsys-main-page-features`. `calculated_at` равен UTC `data_interval_end`.

`start_date = 2026-06-01 00:00 UTC`, `catchup=false`. Для заполнения исторических rolling-окон
нужен отдельный контролируемый backfill.

Iceberg-таблица партиционирована по `hours(calculated_at)`. Повторный запуск идемпотентно
перезаписывает соответствующий 12-часовой срез через `overwritePartitions()`.

Подтверждённый upstream DQ DAG id внешнего источника неизвестен, поэтому отдельный sensor не
добавлен. Ranking upload для Silver-контракта не настраивается.

Пайплайн использует общий Spark image с `git-sync` и resource profile `small`.

## Проверки качества

CI создаёт dbt DQ на уникальность `calculated_at,product_id` и `not_null` ключевых колонок.
Перед записью producer дополнительно проверяет:

- все счётчики неотрицательны;
- `feedback_gte_4 + feedback_lte_3 = feedback_count`;
- `rating_sum >= feedback_count`;
- `rating_sum <= 5 × feedback_count`.

Фильтры producer гарантируют положительный `product_id` и принадлежность исходных строк
целевому полуинтервалу. Рейтинги не фильтруются: нарушения диапазона 1–5 выявляются
перечисленными выше проверками качества.

## Потребители

Rolling feedback-фичи, product ranking-фичи, `neg_feedback_to_orders_rate`,
`neg_feedback_to_orders_rate_smoothed`, feedback percentiles и last-clicked rating profile.

## Владелец и алерты

`table.meta.team = team::recsys`; DAG/alerts team `recsys`; severity `P3`; webhook
`oncall_webhook_recsys`.
