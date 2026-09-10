# Account-L1-category features

DAG id: feature-platform.layers.gold.account_id_l1_category_id.account_l1_category_features.

Airflow group tag: recsys-features.

Целевая таблица: iceberg.gold.feature_platform_account_l1_category_features.

## Контракт

Путь сущности: layers/gold/account_id_l1_category_id/account_l1_category_features/v1.

Grain и primary key: calculated_at,account_id,l1_category_id.

calculated_at — граница Gold snapshot: 00:00 или 12:00 Asia/Tashkent. Строка
публикуется, если у account-category есть action за 28 дней, успешная покупка за
90 дней или impression за 28 дней.

## Product-category mapping

Источник mapping: iceberg.silver.feature_platform_product_metadata. Для всех
rolling-фактов используется S1 snapshot на начало локальной даты calculated_at.
Так один и тот же mapping применяется к actions и заказам данного Gold snapshot,
а история до запуска S1 не выпадает из 60/90-дневных order-окон. Строки без
l1_category_id в G2 не попадают.

## Actions

Источник: iceberg.silver.feature_platform_account_product_session_action_counts_12h.

Одна строка S2c считается один раз в каждом 12-часовом срезе. Одинаковый
session_id,product_id,event_type, попавший в разные срезы, учитывается в каждом
срезе; n_events не суммируется.

PRODUCT_VIEW, ADD_TO_CART и ADD_TO_FAVORITES формируют counts и account-level
ratios за 3, 7, 14 и 28 дней.


## Impressions и conversions

Impressions суммируются из iceberg.silver.feature_platform_account_l1_imp_counts_12h.
Публикуются counts и доли за 3, 7, 14 и 28 дней.

Для click, ATC, ATF и order публикуются три conversion:

- account-category conversion: signal_count / impression_count;
- conversion относительно общего baseline категории;
- conversion относительно общей conversion пользователя.

Для account-level order baseline используется marketplace COUNT(DISTINCT order_id), а
не сумма category counts: один заказ может включать несколько категорий. Нулевой
denominator всегда даёт NULL. Conversion может быть больше 1.

## Заказы и GMV

Позиции читаются из iceberg.silver.order_items, product_id определяется через
iceberg.silver.sku, затем применяется тот же snapshot mapping S1. Учитываются
статусы COMPLETED, PAID, DELIVERED и IN_DELIVERY в полуинтервале
[calculated_at - 90 days, calculated_at).

Для окон 3, 7, 14, 28, 60 и 90 дней:

    l1_n_orders_Nd = COUNT(DISTINCT order_id) на grain категории L1
    l1_gmv_Nd = SUM(payment_price * item_quantity)

Повторные строки одного заказа в category count не дублируют order_id, но весь
GMV позиций сохраняется. Ratios считаются относительно суммы соответствующего
category-level показателя пользователя.


## Recency

По PRODUCT_VIEW за 28 дней публикуется отрицательная целочисленная давность в днях:

    l1_neg_n_days_since_last_click =
        -CEIL((calculated_at - MAX(last_received_at)) / 24 hours)

Relative recency равна recency категории минус наиболее свежее category-recency
пользователя. Самая свежая категория получает 0.

## Запись, зависимости и наблюдаемость

DAG запускается в 07:00 и 19:00 UTC, то есть в 12:00 и 00:00 Asia/Tashkent.
start_date = 2026-08-08T07:00:00Z, catchup=true, resource_profile=small.

До записи DAG ждёт dq владельцев S1 и S2c и S2a. Для
дневного S1 выбирается последний завершённый snapshot, доступный на границе G2.
Для внешних iceberg.silver.order_items и iceberg.silver.sku подтверждённые
upstream DQ DAG ids не заданы.

MERGE полностью синхронизирует только текущий calculated_at. Таблица
партиционирована по days(calculated_at). После записи параллельно запускаются dq и
feature_stats. DQ проверяет ключ, положительные ID, неотрицательные counts, GMV и
conversions, ratios в диапазоне [0,1] и неположительную recency. Пороги freshness
и объёма на первичной раскатке имеют severity warn.

Потребители: Main, push, train и candidate enrichment. L2 имеет активного
push-ranking consumer. Ranking upload в этом контракте пока не настраивается.
