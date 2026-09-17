# Account-L2-category features

DAG id: feature-platform.layers.gold.account_id_l2_category_id.account_l2_category_features.

Airflow group tag: recsys-features.

Alert уровня P3 для команды recsys через oncall_webhook_recsys полностью
настроен, но callback основного DAG, DQ и feature_stats временно отключены на
период отладки.

Целевая таблица: iceberg.gold.feature_platform_account_l2_category_features.

## Контракт

Путь сущности: layers/gold/account_id_l2_category_id/account_l2_category_features/v1.

Grain и primary key: calculated_at,account_id,l2_category_id.

Идентификаторы и счётчики в физическом контракте имеют тип INT.

Namespace контракта — `ACCOUNT_L2`. Все физические feature-колонки уже содержат
его, например `ACCOUNT_L2__n_clicks_7d`; устаревший префикс `l2_` не
используется. Ключи `calculated_at`, `account_id` и `l2_category_id` остаются
без namespace.

calculated_at — граница Gold snapshot: 00:00 или 12:00 Asia/Tashkent. Строка
публикуется, если у account-category есть action за 28 дней, успешная покупка за
90 дней или impression за 28 дней.

## Product-category mapping

Источник mapping: iceberg.silver.feature_platform_product_metadata. Для всех
rolling-фактов используется S1 snapshot на начало локальной даты calculated_at.
Так один и тот же mapping применяется к actions и заказам данного Gold snapshot,
а история до запуска S1 не выпадает из 60/90-дневных order-окон. Строки без
l2_category_id в G2 не попадают.

## Actions

Источник: iceberg.silver.feature_platform_account_product_session_action_counts_12h.

Перед агрегацией строки S2c дедуплицируются на полном 28-дневном окне по
account_id,session_id,product_id,event_type. Одна товаро-сессия учитывается один
раз, даже если пересекает границу двух 12-часовых срезов; n_events не суммируется.

PRODUCT_VIEW, ADD_TO_CART и ADD_TO_FAVORITES формируют counts и account-level
ratios за 3, 7, 14 и 28 дней.


## Impressions и conversions

Impressions суммируются из iceberg.silver.feature_platform_account_l2_imp_counts_12h.
Публикуются counts и доли за 3, 7, 14 и 28 дней.

Для click, ATC, ATF и order публикуются четыре семейства conversion:

- `conv_imp2{signal}_raw_{window}d`: исходная account-category conversion,
  `signal_count / impression_count`;
- `overall_conv_imp2{signal}_raw_{window}d`: исходная conversion
  пользователя по всем категориям L2;
- `conv_imp2{signal}_div_total_category_conv_{window}d`: исходная
  account-category conversion, делённая на общий baseline этой категории;
- `conv_imp2{signal}_div_overall_conv_{window}d`: account-category
  conversion, делённая на общую conversion пользователя по всем категориям.

`overall_conv_imp2order_raw_{window}d` и соответствующий относительный
признак используют marketplace `COUNT(DISTINCT order_id)`, а не сумму category
counts: один заказ может включать несколько категорий. Нулевой denominator
всегда даёт NULL. Conversion может быть больше 1.

## Заказы и GMV

Позиции читаются из iceberg.silver.order_items, product_id определяется через
iceberg.silver.sku, затем применяется тот же snapshot mapping S1. Учитываются
статусы COMPLETED, PAID, DELIVERED и IN_DELIVERY в полуинтервале
[calculated_at - 90 days, calculated_at).

B2B-позиции исключаются условием order_items.b2b_order = FALSE. Корректность
идентификаторов order_items и sku считается гарантией Silver; Gold не повторяет
range-фильтры входных ID. Положительность ключей проверяется после записи.

Для окон 3, 7, 14, 28, 60 и 90 дней:

    n_orders_Nd = COUNT(DISTINCT order_id) на grain категории L2
    gmv_Nd = SUM(payment_price * item_quantity)

Повторные строки одного заказа в category count не дублируют order_id, но весь
GMV позиций сохраняется. Ratios считаются относительно суммы соответствующего
category-level показателя пользователя.

## Запись, зависимости и наблюдаемость

DAG запускается в 07:00 и 19:00 UTC, то есть в 12:00 и 00:00 Asia/Tashkent.
start_date = 2026-09-05T07:00:00Z — первый snapshot после накопления полного
28-дневного окна Silver; catchup=true, resource_profile=small.

До записи DAG ждёт dq владельцев S1 и S2c и S2b. Для
дневного S1 выбирается последний завершённый snapshot, доступный на границе G2.
Для внешних iceberg.silver.order_items и iceberg.silver.sku подтверждённые
upstream DQ DAG ids не заданы.

MERGE полностью синхронизирует только текущий calculated_at. Таблица
партиционирована по days(calculated_at). После записи параллельно запускаются dq и
feature_stats. DQ проверяет ключ, положительные ID, неотрицательные counts, GMV и
conversions, конечность conversions и ratios в диапазоне [0,1]. Пороги freshness
и объёма на первичной раскатке имеют severity warn. Alert callbacks остаются
отключёнными до окончания отладки.

Потребители: Main, push, train и candidate enrichment. L2 имеет активного
push-ranking consumer. Ranking upload в этом контракте пока не настраивается.
