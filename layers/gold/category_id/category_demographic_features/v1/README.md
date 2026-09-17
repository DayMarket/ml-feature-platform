# Leaf-category demographic features

DAG id: `feature-platform.layers.gold.category_id.category_demographic_features`.

Таблица: `iceberg.gold.feature_platform_category_demographic_features`.

Grain и primary key: `calculated_at,category_id`.

Namespace — `CATEGORY_DEMOGRAPHICS`. Все физические feature-колонки уже содержат
его; `calculated_at` и `category_id` остаются без namespace.

## Назначение

G6 хранит gender- и age-статистики просмотров на уровне листовой категории.
Account-category строки и candidate enrichment в таблицу не материализуются.

## Источники

- S2c `iceberg.silver.feature_platform_account_product_session_action_counts_12h`;
- S1 `iceberg.silver.feature_platform_product_metadata`;
- S5 `iceberg.silver.feature_platform_account_demographics`.

S1 присоединяется по `product_id`, S5 — по `account_id`. Листовая категория
берётся из `S1.category_id`, а её нормализованный gender `M`, `F`, `U` или
`NULL` — из `S1.category_gender`. Возраст и gender account берутся из S5.
Поэтому исторический расчёт использует
согласованный snapshot S1 и не обращается к актуальному справочнику напрямую.

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

После дедупликации присоединяется S1 текущей локальной даты. Строки без
листовой `category_id` исключаются. S5 присоединяется через `LEFT JOIN`:
просмотры пользователей без известного gender сохраняют категорию в
population, но не входят в gender denominator.

## Unique-clicker семантика

Для unique-user и age-статистик product-session наблюдения дополнительно
дедуплицируются до одной строки на `account_id,category_id` на полном 28-дневном
окне. Поэтому пользователь, просмотревший один или сто товаров категории,
получает один голос. Age-перцентили используют пользователей с возрастом от 13
до 100 лет включительно; пользователи без валидного возраста остаются в общем
`n_unique_clickers_28d`, но не входят в age-распределение.

## Колонки и формулы

- `CATEGORY_DEMOGRAPHICS__female_product_session_share_28d` — доля female product-session
  наблюдений среди product-session наблюдений с известным gender;
- `CATEGORY_DEMOGRAPHICS__male_product_session_share_28d` — аналогичная male-доля;
- `CATEGORY_DEMOGRAPHICS__female_unique_clicker_share_28d` и
  `CATEGORY_DEMOGRAPHICS__male_unique_clicker_share_28d` — gender-доли среди
  уникальных пользователей с известным gender;
- `CATEGORY_DEMOGRAPHICS__gender_balance_28d` — непрерывная сбалансированность
  product-session gender-аудитории от 0 до 1;
- `CATEGORY_DEMOGRAPHICS__n_unique_clickers_28d` — все уникальные account категории;
- `CATEGORY_DEMOGRAPHICS__n_unique_known_gender_clickers_28d` — уникальные account с gender `MALE` или
  `FEMALE`;
- `CATEGORY_DEMOGRAPHICS__n_unique_female_clickers_28d` — уникальные female account;
- `CATEGORY_DEMOGRAPHICS__n_unique_male_clickers_28d` — уникальные male account;
- `CATEGORY_DEMOGRAPHICS__n_unique_clickers_with_age_28d` — уникальные account с
  валидным возрастом;
- `CATEGORY_DEMOGRAPHICS__known_age_clicker_share_28d` — покрытие валидного возраста;
- `CATEGORY_DEMOGRAPHICS__clicker_age_p10_28d`,
  `CATEGORY_DEMOGRAPHICS__clicker_age_p50_28d` и
  `CATEGORY_DEMOGRAPHICS__clicker_age_p90_28d` — точные перцентили возраста
  уникальных пользователей; p50 является медианным возрастом;
- `CATEGORY_DEMOGRAPHICS__gender` — `M`, `F`, `U` или `NULL` для листовой
  категории.

```text
female_share = female product-session rows / known-gender product-session rows
male_share   = male product-session rows / known-gender product-session rows
female_unique_share = unique female accounts / unique known-gender accounts
male_unique_share   = unique male accounts / unique known-gender accounts
gender_balance      = 1 - 2 * abs(female_share - 0.5)
known_age_share     = unique accounts with valid age / all unique accounts
```

Если в категории нет product-session наблюдений с известным gender, обе доли
равны `NULL`, а unique counts равны нулю. Для опубликованной категории:

```text
CATEGORY_DEMOGRAPHICS__n_unique_known_gender_clickers_28d
    = CATEGORY_DEMOGRAPHICS__n_unique_female_clickers_28d + CATEGORY_DEMOGRAPHICS__n_unique_male_clickers_28d
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
Asia/Tashkent. Он ждёт DQ-таски S1, S2c и S5.

`start_date = 2026-09-05T07:00:00Z` — первый запуск после накопления полного
28-дневного окна S2c; `catchup=true`. DAG создаётся на паузе. Group tag:
`recsys-features`.

Spark запускается с `resource_profile: small`. Alert routing P3 настроен, но
callbacks DAG, DQ и feature stats отключены на время отладки.

## DQ and feature stats

DQ проверяет уникальность ключа, положительный `category_id`, неотрицательные counts,
домены gender, диапазоны shares, сумму female/male shares и равенство known
unique count сумме female/male unique counts. Дедупликация product-session
и отдельная дедупликация unique clickers зафиксированы SQL unit-тестами. Также
проверяются диапазон и порядок `age_p10 <= age_p50 <= age_p90`, age coverage и
сумма female/male unique shares.

`feature_stats` выполняет отдельный Trino-скан каждого 12-часового snapshot;
строковый `CATEGORY_DEMOGRAPHICS__gender` исключён из профилирования. Ranking upload не
настраивается. Потребители: G7 и candidate enrichment для Main, push и train.

После merge в `master` dbt PR не создаётся (`create_dbt_pr: false`), а CI может
создать maintenance PR в `DayMarket/pyspark-etl`
(`create_maintenance_pr: true`).
