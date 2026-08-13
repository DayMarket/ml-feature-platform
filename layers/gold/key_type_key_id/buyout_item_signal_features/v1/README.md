# iceberg.gold.feature_platform_buyout_item_signal_features

Товарный сигнал выкупаемости в длинном формате: одна строка на пару «уровень × ID»
(`sku`, `product`, `category`, `shop`, `brand`) со счётчиками и сырыми ставками в окнах
30 и 90 дней. Базовая витрина модели невыкупов: из неё собираются online-проекции.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_buyout_item_signal_features`.
- DAG: `feature-platform.layers.gold.key_type_key_id.buyout_item_signal_features`
  (`layers/gold/key_type_key_id/buyout_item_signal_features/v1/dag.py`).
- Групповой тег Airflow: `buyout-features`.
- Расписание: ежедневно в 03:00 UTC, `0 3 * * *`.
- `start_date=2026-08-20T00:00:00Z`, `catchup=False`, `max_active_runs=1`.

## Грейн / ключ

`date, key_type, key_id`.

`date` равна `analyze_date` источника и вычисляется как `data_interval_end - 1 day` в UTC.
`key_type` задаёт уровень агрегации, `key_id` — идентификатор на этом уровне. Один и тот же
числовой ID на разных уровнях не пересекается по смыслу: читать таблицу нужно всегда
с фильтром по `key_type`.

## Источники

- `"dwh-iceberg".silver.history_order_items` — снапшот истории заказов на `analyze_date`;
- `"dwh-iceberg".silver.sku` — привязка `sku_id` к `product_id`, `shop_id`, `brand_name_id`.

Обе таблицы — внешние DE-источники.

## Зависимости

Сенсоров нет. `history_order_items` — внешний DE-источник; его снапшот за
`analyze_date = D` публикуется в `D 19:00 UTC`, то есть за 8 часов до запуска DAG за `D`.
Пустой результат источника не пишется: `require_non_empty` останавливает задачу с ошибкой, чтобы не затереть
хорошую партицию.

## Логика

Внутри снапшота `analyze_date = date` берутся позиции с `generated_at` за последние 90 дней
(граница переводится в `Asia/Tashkent`), статус `ACTIVE` исключён: исход таких позиций
неизвестен, в знаменателе они занижали бы выкупаемость свежих товаров. Окно 30 дней
считается тем же сканом через `FILTER (WHERE in_30d = 1)`.

Классификация исходов — канон `target_orders.sql` (MAD-13227):

- доставка: `delivered_at IS NOT NULL OR (delivery_type = 'COURIER' AND issued_at IS NOT NULL)`;
- клиентский невыкуп: `RETURNED NO SHOW` или (`RETURNED` с `return_cause = 'CANCELED'`);
- возврат по вине маркетплейса или контента — отдельный счётчик `n_fair_return_90d`
  (`MISSING`, `DEFECTED`, `BAD_QUALITY`, `WRONG_ITEM`, `PHOTO_MISMATCH`, `CONTENT`).

Пять уровней собираются одним `GROUPING SETS`. `key_id` разбирается явным `CASE ... GROUPING(...)`,
а не `COALESCE`: при пропуске в `silver.sku` ключ соседнего уровня подставился бы молча и
агрегаты смешались бы между гранулярностями.

Ставки сырые, без сглаживания: сглаживание к родителю (k = 30) делает online-проекция
`buyout_online_sku_features`. Нулевой знаменатель даёт `NULL`, а не ноль.

## Caveats

- `WHERE key_id IS NOT NULL` отбрасывает уровни, которых нет в `silver.sku` (например,
  позиции без `product_id` или `brand_name_id`). Такие позиции всё равно попадают в
  агрегаты уровня `sku` и `category`.
- Денежная выкупаемость считается по `gmv_generated` (UZS), а не по финальному GMV: это
  сигнал товара, а не сверка юнит-экономики.
- Атрибуты `silver.sku` берутся текущим снимком: изменение категории или магазина у SKU
  задним числом переносит всю его историю на новый уровень.
- Один `order_id` может содержать позиции с разными исходами, поэтому счётчики
  считаются по позициям, а не по заказам.

## Рантайм

Trino-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Чтение через connection
`trino_search`, запись — через entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, 16Gi / 4 CPU.

Выбор `trino_search` — вопрос PR: витрина не относится к поисковому домену, но у buyer-команды
пока нет отдельного Trino-connection.

Запись идемпотентна: партиция `date` перезаписывается целиком через PyIceberg `overwrite`.

## Владелец / алерты

`table.meta.team = team:buyer`, alerts `buyer`, severity P2, webhook `oncall_webhook_buyer`.
Идентификатор on-call webhook подтверждается в PR.
