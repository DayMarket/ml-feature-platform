# L6 category gender features

DAG id: `feature-platform.layers.gold.l6_category_id.l6_category_gender_features`.

Таблица: `iceberg.gold.feature_platform_l6_category_gender_features`.

Grain и primary key: `calculated_at,l6_category_id`.

Логический namespace — `L6_CATEGORY_GENDER`. Физические feature-колонки
хранятся без namespace в lower snake case.

## Назначение

G6 хранит gender-статистики просмотров на уровне L6. Account-category строки и
candidate enrichment в таблицу не материализуются.

## Источники

- S2c `iceberg.silver.feature_platform_account_product_session_action_counts_12h`;
- S1 `iceberg.silver.feature_platform_product_metadata`;
- S5 `iceberg.silver.feature_platform_account_demographics`;
- `iceberg.silver.recsys_category_genders`.

S1 присоединяется по `product_id`, S5 — по `account_id`, справочник gender
категории — по `S1.l6_category_id = recsys_category_genders.category_id`.
Используется `dominant_gender`, нормализованный до `M`, `F`, `U` или `NULL`.

`S1.category_gender` намеренно не используется: он относится к листовой
`category_id`, которая может отличаться от `l6_category_id`. Справочник
`recsys_category_genders` не партиционирован, поэтому исторические пересчёты
используют его актуальное состояние.

## Product-session семантика

Из S2c берутся только `PRODUCT_VIEW` на полуоткрытом окне
`[calculated_at - 28 days, calculated_at)`. Строки всех 12-часовых срезов
дедуплицируются на полном окне по:

```text
account_id, session_id, product_id
```

Повторные просмотры одного товара в одной сессии дают одно наблюдение. Один
товар в разных сессиях и разные товары в одной сессии дают разные наблюдения.
`n_events` не используется.

После дедупликации присоединяется S1 текущей локальной даты. Строки без L6
исключаются. S5 присоединяется через `LEFT JOIN`: просмотры пользователей без
известного gender сохраняют категорию в population, но не входят в gender
denominator.

## Колонки и формулы

- `category_female_product_session_share_28d` — доля female product-session
  наблюдений среди product-session наблюдений с известным gender;
- `category_male_product_session_share_28d` — аналогичная male-доля;
- `n_unique_known_gender_clickers_28d` — уникальные account с gender `MALE` или
  `FEMALE`;
- `n_unique_female_clickers_28d` — уникальные female account;
- `n_unique_male_clickers_28d` — уникальные male account;
- `category_gender` — `M`, `F`, `U` или `NULL` для L6.

```text
female_share = female product-session rows / known-gender product-session rows
male_share   = male product-session rows / known-gender product-session rows
```

Если в категории нет product-session наблюдений с известным gender, обе доли
равны `NULL`, а unique counts равны нулю. Для опубликованной категории:

```text
n_unique_known_gender_clickers_28d
    = n_unique_female_clickers_28d + n_unique_male_clickers_28d
```

## On-the-fly candidate enrichment

Следующие признаки вычисляются после соединения кандидата с S1, G5 и G6 на том
же `calculated_at`; физическими колонками G5 или G6 они не являются:

```text
account_gender_category_click_share_28d
account_gender_mismatch_category
account_category_female_click_share_abs_diff_28d
```

Первый выбирает female- или male-share категории по gender account. Второй
сравнивает gender account и категории, считая `U` совместимым с обоими. Третий
равен абсолютной разнице между female-share категории и
`G5.last_clicked_female_cat_share_among_gendered`.

## Snapshot time semantics

Gold `calculated_at` и S2c используют локальный clock-value `00:00`/`12:00`
Asia/Tashkent. S5 читается по `00:00` локальной даты. S1 физически хранит начало
той же локальной даты как UTC-момент `19:00` предыдущего дня; запрос учитывает
это различие явно.

## Orchestration and dependencies

DAG работает в `07:00` и `19:00 UTC`, то есть `12:00` и `00:00`
Asia/Tashkent. Он ждёт DQ-таски S1, S2c и S5. Для внешнего текущего справочника
`recsys_category_genders` отдельный feature-platform DQ sensor отсутствует.

`start_date = 2026-09-05T07:00:00Z` — первый запуск после накопления полного
28-дневного окна S2c; `catchup=true`. DAG создаётся на паузе. Group tag:
`recsys-features`.

Spark запускается с `resource_profile: small`. Alert routing P3 настроен, но
callbacks DAG, DQ и feature stats отключены на время отладки.

## DQ and feature stats

DQ проверяет уникальность ключа, положительный L6, неотрицательные counts,
домены gender, диапазоны shares, сумму female/male shares и равенство known
unique count сумме female/male unique counts. Дедупликация product-session
зафиксирована SQL unit-тестами.

`feature_stats` выполняет отдельный Trino-скан каждого 12-часового snapshot;
строковый `category_gender` исключён из профилирования. Ranking upload не
настраивается. Потребители: G7 и candidate enrichment для Main, push и train.

После merge в `master` dbt PR не создаётся (`create_dbt_pr: false`), а CI может
создать maintenance PR в `DayMarket/pyspark-etl`
(`create_maintenance_pr: true`).
