# Gold Ad Revenue Features по SKU Group ID

Пайплайн собирает финальные daily-признаки среднего заработка с рекламы на уровне `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_sku_group_ad_revenue_features`.

Грейн: `date, sku_group_id` (первичный ключ).

## Источник

- `iceberg.silver.feature_platform_sku_group_ad_revenue_daily` — дневной silver pre-aggregate рекламных показов и выручки по `sku_group_id` (см. README соответствующего silver-слоя).

«Заработок с рекламы» = `adrev` (рекламные расходы продавца) из исходного `adv_funnel_daily`. Это **весь рекламный CPC-funnel** (поисковая выдача + категории), без выделения строго `SEARCH_RESULTS` — такого разреза в источнике нет.

## Окна

Окна считаются строго до даты расчета: для `ds = 2026-06-08` окно `7` использует даты `[2026-06-01, 2026-06-07]`. Сам `ds` не входит в расчет (как в `sku_group_search_conversion_features`).

## Признаки

Для каждого окна `w` ∈ {7, 14, 30}:

- `adrev_per_imp_w` — raw средний заработок с рекламы на показ:

  ```text
  adrev_per_imp_w = sum_adrev_w / sum_impressions_w
  ```

  Деление оставляет Spark-семантику `NULL` при нулевом или отсутствующем знаменателе (показатель не подменяется на `0.0`).

- `smooth_adrev_per_imp_w` — сглаженный средний заработок с рекламы на показ (аддитивное сглаживание к глобальному среднему):

  ```text
  smooth_adrev_per_imp_w = (PRIOR_ADREV + sum_adrev_w) / (PRIOR_IMP + sum_impressions_w)
  PRIOR_MEAN_ADREV_PER_IMP = 19.9   # глобальное среднее adrev/impression
  PRIOR_IMP                = 50.0   # сила приора в псевдо-показах
  PRIOR_ADREV              = 995.0  # = PRIOR_MEAN_ADREV_PER_IMP * PRIOR_IMP
  ```

  Сглаженный вариант не возвращает `NULL`: при отсутствии показов значение стягивается к глобальному среднему. Константы оценены по `silver.adv_funnel_daily` за 30 дней и подлежат настройке владельцем модели.

## Оркестрация

Gold DAG ждет DQ-DAG silver-источника:
`dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_ad_revenue_daily.dq`
(а не Spark-DAG, который пишет silver-таблицу).

## Downstream

Ranking-upload не настроен — таблица пока используется как внутренняя feature-таблица. При публикации в ranking-service добавить группу в `upload/ranking_features/v1` (entity key `sku_group_id`).

## Runtime

Дефолтный Spark image + `git-sync` initContainer. Код запускается из
`/git/repo/layers/gold/sku_group_ad_revenue_features/v1/entrypoints/get_sku_group_ad_revenue_features.py`,
отдельный Docker image не собирается.
