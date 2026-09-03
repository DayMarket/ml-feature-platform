# 12-часовые feedback-факты товара

DAG id: `feature-platform.layers.silver.product_id.product_feedback_counts_12h`.

Airflow group tag: `recsys-features`.

Целевая таблица: `iceberg.silver.feature_platform_product_feedback_counts_12h`.

## Контракт

Путь сущности:
`layers/silver/product_id/product_feedback_counts_12h/v1`.

Grain и primary key: `calculated_at,product_id`.

- `calculated_at` — правая граница 12-часового интервала в `Asia/Tashkent`;
- `product_id` — положительный ID товара;
- `n_feedbacks_1` — количество опубликованных отзывов с рейтингом 1;
- `n_feedbacks_2` — количество опубликованных отзывов с рейтингом 2;
- `n_feedbacks_3` — количество опубликованных отзывов с рейтингом 3;
- `n_feedbacks_4` — количество опубликованных отзывов с рейтингом 4;
- `n_feedbacks_5` — количество опубликованных отзывов с рейтингом 5.

Общее количество отзывов, сумма рейтингов и объединённые диапазоны рейтинга в S4 не
хранятся: эти показатели выводятся из пяти базовых счётчиков в Gold. SKU, SKU-group, средний
рейтинг и all-time показатели в S4 также не публикуются.

## Источник и окно

Источник: `iceberg.silver_bxappdb2_foodback.public_feedback`.

Timestamp события — `date_published`. Airflow передаёт границу интервала в UTC, после чего
producer переводит обе границы в `Asia/Tashkent`. Spark session также работает в
`Asia/Tashkent`, и запрос использует полуоткрытый интервал:

```text
[calculated_at - 12 hours, calculated_at)
```

Фильтры:

- `status = 'PUBLISHED'`;
- `product_id > 0`;
- `date_published >= calculated_at - 12 hours`;
- `date_published < calculated_at`.

Статус `PUBLISHED` и длительность среза 12 часов являются фиксированной частью S4 и не
параметризуются через `config.yaml`.

Контракт предполагает, что опубликованный отзыв не возвращается в `UNPUBLISHED`, не
публикуется повторно и физически не удаляется. Изменения рейтинга после публикации намеренно
не пересчитывают старые 12-часовые срезы.

## Расчёт и rolling-семантика

Для каждого `product_id` считаются:

```text
n_feedbacks_1 = COUNT(rating = 1)
n_feedbacks_2 = COUNT(rating = 2)
n_feedbacks_3 = COUNT(rating = 3)
n_feedbacks_4 = COUNT(rating = 4)
n_feedbacks_5 = COUNT(rating = 5)
```

Значения рейтинга 1–5 являются фиксированной частью контракта и не параметризуются через
`config.yaml`. Исходные строки по рейтингу не фильтруются: producer сравнивает сумму пяти
счётчиков с общим количеством непустых рейтингов и останавливает запись при значении вне
диапазона 1–5.

Все пять показателей аддитивны. Сначала Gold суммирует каждый счётчик по нужному rolling-окну,
после чего рассчитывает производные показатели:

```text
feedback_count = n_feedbacks_1 + n_feedbacks_2 + n_feedbacks_3
    + n_feedbacks_4 + n_feedbacks_5
rating_sum = n_feedbacks_1 + 2 * n_feedbacks_2 + 3 * n_feedbacks_3
    + 4 * n_feedbacks_4 + 5 * n_feedbacks_5
feedback_lte_3 = n_feedbacks_1 + n_feedbacks_2 + n_feedbacks_3
feedback_gte_4 = n_feedbacks_4 + n_feedbacks_5
rating = rating_sum / feedback_count
```

Для среднего рейтинга Gold проверяет, что знаменатель `feedback_count` не равен нулю.

All-time показатели не рассчитываются через S4. Для них сохраняется существующий контракт
`iceberg.gold.feature_platform_product_feedback_base_stats`.

## Оркестрация и хранение

DAG запускается в `07:00` и `19:00 UTC`, синхронно с другими 12-часовыми Silver-срезами группы
`recsys-features`. `calculated_at` равен `data_interval_end`, переведённому из UTC в
`Asia/Tashkent`.

`start_date = 2026-08-08 07:00 UTC`, `catchup=true`: при первом включении DAG выполняет
начальный backfill примерно за две недели.

Iceberg-таблица партиционирована по `days(calculated_at)`. Одна дневная партиция содержит до
двух 12-часовых срезов. Повторный запуск идемпотентно перезаписывает только строки конкретного
`calculated_at`, не удаляя второй срез того же дня.

Подтверждённый upstream DQ DAG id внешнего источника неизвестен, поэтому отдельный sensor не
добавлен. Ranking upload для Silver-контракта не настраивается.

Пайплайн использует общий Spark image с `git-sync` и resource profile `small`.

## Проверки качества

После записи параллельно запускаются внутренние `dq` и `feature_stats` для конкретного
`calculated_at`. DQ блокирует downstream при NULL или дублях ключа
`calculated_at,product_id`; freshness, минимальный объём и изменение объёма во время первичной
раскатки имеют severity `warn`. Перед записью producer дополнительно проверяет:

- каждый из пяти счётчиков неотрицателен;
- сумма `n_feedbacks_1`…`n_feedbacks_5` равна количеству всех непустых рейтингов в срезе.

Фильтры producer гарантируют положительный `product_id` и принадлежность исходных строк
целевому полуинтервалу. Рейтинги не фильтруются: значение вне диапазона 1–5 выявляется до
записи сравнением с техническим счётчиком, который не публикуется в таблицу.

`feature_stats` считает распределения пяти счётчиков одним полным Trino-сканом записанного
12-часового среза на каждый запуск, то есть два скана в сутки.

`table.meta.create_dbt_pr: false`: CI не создаёт для таблицы новый DQ-PR в `dbt-trino`;
Iceberg maintenance остаётся включённым.

## Потребители

Rolling feedback-фичи, product ranking-фичи, `neg_feedback_to_orders_rate`,
`neg_feedback_to_orders_rate_smoothed`, feedback percentiles и last-clicked rating profile.

## Владелец и алерты

`table.meta.team = team::recsys`; DAG/alerts team `recsys`; severity `P3`; webhook
`oncall_webhook_recsys`.
