# Gold Ad Rating Features по категориям

Пайплайн считает средний рейтинг рекламируемых товаров на уровне `category_id` за последние 30 дней с экспоненциальным затуханием по дням назад.

Целевая таблица: `iceberg.gold.feature_platform_category_ad_rating_features`.

Грейн: `date, category_id` (первичный ключ).

## Источники

- `iceberg.silver.feature_platform_sku_group_ad_revenue_daily` — дневные рекламные показы по `sku_group_id` (весь рекламный CPC-funnel, без разреза по площадке). «Рекламируемый товар» в день `d` = `sku_group_id` с `ad_impressions > 0`.
- `iceberg.silver_bxappdb2_foodback.public_feedback` — сырые отзывы (`status = 'PUBLISHED'`), джойн на `iceberg.silver.sku` по `sku_id` для маппинга в `sku_group_id` (та же логика, что в `gold/feedback_sku_group_id`).
- `iceberg.silver.sku` — маппинг `sku_group_id -> category_id` (distinct, как в `gold/sku_group_price_features`). Если sku_group входит в несколько категорий, он учитывается в каждой.

## Формула

Окно: 30 дней строго до даты расчета, `[ds - 30, ds - 1]`; сам `ds` не входит.

Для каждого дня `d` окна и каждого рекламируемого в этот день `sku_group`:

- рейтинг берется **на момент дня показа**: средний `rating` опубликованных отзывов с `date_published < d` (без утечки из будущего). Товары без отзывов на день `d` в среднее не входят;
- вес наблюдения — экспоненциальное затухание по возрасту дня:

  ```text
  age    = ds - 1 - d            # 0..29
  weight = 0.5 ^ (age / 14)      # half-life 14 дней
  ```

- внутри дня все рекламируемые товары имеют равный вес (объем показов не учитывается). Товар, рекламировавшийся несколько дней, входит по одному наблюдению на каждый день со своим весом.

Признак категории:

```text
avg_advertised_rating_30d_hl14 =
    sum(weight * rating_as_of_day) / sum(weight)   # по наблюдениям с рейтингом
```

`NULL`, если ни один рекламируемый товар категории не имел отзывов (знаменатель не подменяется на `0.0`).

Вспомогательные колонки: `advertised_sku_groups_30d` (уникальные рекламируемые sku_group категории за окно) и `rated_advertised_sku_groups_30d` (из них — с хотя бы одним отзывом на день показа).

Реализация as-of рейтинга: отзывы до начала окна сворачиваются в базовую сумму/количество на `sku_group`, отзывы внутри окна — в дневные суммы; рейтинг на день `d` собирается из базы плюс дневных сумм с `pub_date < d`. Это избавляет от джойна полной истории отзывов на каждый день окна.

## Оркестрация

Gold DAG ждет DQ-DAG silver-источника:
`dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_ad_revenue_daily.dq`
(а не Spark-DAG, который пишет silver-таблицу).

Внешние DE-таблицы `public_feedback` и `silver.sku` сенсорами не покрываются — как в существующих `feedback_sku_group_id` и `sku_group_price_features`.

## DQ

Автогенерируемые dbt-тесты (uniqueness/not_null по PK, freshness, row-count). Дополнительно осмысленный контрактный тест — диапазон `avg_advertised_rating_30d_hl14` в `[1, 5]`; добавляется в dbt-trino отдельно, в этом репо не настраивается.

## Downstream

Ranking-upload не настроен — таблица используется как внутренняя feature-таблица. Чистый entity key `category_id` сейчас не входит в поддерживаемые ключи ranking upload; публикация потребует расширения контракта upload-валидации.

## Runtime

Дефолтный Spark image + `git-sync` initContainer. Код запускается из
`/git/repo/layers/gold/category_ad_rating_features/v1/entrypoints/get_category_ad_rating_features.py`,
отдельный Docker image не собирается.
