# iceberg.gold.feature_platform_location_h3_forecast_features

Широкая h3-витрина признаков локаций — **единый источник всех фич** для моделей семейства
`location_forecast`. Содержит полный набор сырых silver-колонок (feature store) **плюс** стабильный
контракт фич модели `location_forecast` (имена совпадают с `features_to_use`, без per-prediction
временных фич, которые модель добавляет на инференсе). Разные модели выбирают свой подмножество колонок
на чтении; витрина не сужается под одну модель.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_location_h3_forecast_features`.
- DAG: `feature-platform.layers.gold.h3_index.location_h3_forecast_features` (`layers/gold/h3_index/location_h3_forecast_features/v1/dag.py`).
- Групповой тег Airflow: `location-h3-forecast`.
- Расписание: ежедневно в 02:00 UTC, `start_date=2026-06-19T00:00:00Z`.

## Грейн / ключ
`date, h3_index`.

## Источник
**Full-outer union** пяти silver-таблиц на `(date, h3_index)` — так в витрину попадает каждый гекс,
у которого есть хотя бы одна фича, ни одна silver-строка не теряется:
`iceberg.silver.feature_platform_geo_geointellect_features`,
`iceberg.silver.feature_platform_dp_neighbor_order_features`,
`iceberg.silver.feature_platform_geo_user_activity_features`,
`iceberg.silver.feature_platform_geo_user_location_features` и
`iceberg.silver.feature_platform_geo_yandex_poi_features`.
`h3_string`/`region` берутся из geointellect (у `dp_neighbor` одноимённый `h3_string`
отбрасывается перед джойном, чтобы не плодить `_x/_y`).

Gold DAG ждёт отдельный dbt DQ DAG каждой из пяти silver-таблиц. DQ DAG стартуют в
01:00 UTC, поэтому gold в 02:00 UTC ищет их logical date с `execution_delta=1 hour`. Только после
успеха всех пяти сенсоров он проверяет входные и выходную таблицы через PyIceberg и собирает gold.

## Логика
Колонки витрины делятся на три группы:

1. **Сырые silver-колонки** — весь набор из пяти silver-таблиц без изменений имён (полная сетка
   `orders_r{0..5}_h{30,60,90}`, `gmv_*`, `unique_dp_*`, `views_r{0..5}_{30,60,90}d`,
   `orders_r{0..5}_{30,60,90}d`, `population_r{0..5}`/`_l{5..8}`, `pedestrian_traffic_index_*`,
   `users_r{0..5}`, POI `*_r{0..5}`, `report_date`, `min_dist_*`).
2. **Контрактные копии модели** — стабильные имена, продублированные из сырой колонки (сырьё
   сохраняется рядом): `population` (= `population_r0`), `pedestrian_traffic_index` (= `..._r0`),
   `users_views_r1_30d` (= `views_r1_30d`), `users_orders_30d` (= `orders_r0_30d`),
   `users_orders_r3_90d` (= `orders_r3_90d`), `users_orders_r4_30d` (= `orders_r4_30d`),
   `atms_rad_1` (= `atms_r1`), `banks_rad_2` (= `banks_r2`), `retail_points_rad_1`,
   `car_dealers_services_rad_2`, `mixed_goods_rad_2`, `fast_food_coffee_rad_5`, `bakeries_rad_1`.
3. **Производные**: `traffic_ring_1_2 = ptr_r2 - ptr_r1`,
   `orders_per_dp_r5_h90 = orders_r5_h90 / (unique_dp_r5_h90 + 1e-5)`.

Дефолты: `region` NULL -> `UNKNOWN`, дистанции NULL -> 10000, прочие **числовые** NULL -> 0;
строковые/дата-колонки (`h3_string`, `report_date`) остаются NULL там, где источник не покрыл гекс.
Витрина **не** применяет candidate-фильтр (`population_r1 > 0 OR pedestrian_traffic_index_r1 > 0`) —
это делает потребитель на чтении, поэтому строк в gold заметно больше, чем в отфильтрованном
инференс-датасете. Это **iceberg-source** задача (читает silver, не ClickHouse).

## Рантайм

Iceberg-source пайплайн (Airflow/Python + `pyiceberg`), **не** Spark и не ClickHouse. Чтение silver,
сборку и запись gold выполняют entity-local модули `job/runtime.py` и `job/build.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.
Каталог Iceberg (Hive metastore, warehouse, YC S3) настроен идентично Spark-шаблону
`config/spark/layer_spark_application.yaml`; ключи S3 берутся из connection `spark_ycs_connection`.

Идентификаторы всех таблиц строятся из соответствующих `config.yaml` как tuple `(schema, table)`.
Запись идемпотентна: партиция `date` перезаписывается целиком (`overwrite` по фильтру `date`).

## Миграции

- `migrations/create_table.sql` — полная актуальная DDL витрины (180 колонок: `date`, `h3_index`,
  метаданные + все три группы фич из раздела «Логика»). Используется для чистых окружений;
  `CREATE TABLE IF NOT EXISTS`, партиционирование по `date`.
- `migrations/20260702_add_all_silver_feature_columns.sql` — расширение витрины до **единого источника
  всех фич**. Изначально gold содержал только модельный контракт (переименования/производные/дефолты,
  ~52 колонки). Эта миграция **аддитивно** добавляет полный набор сырых silver-колонок
  (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS` — full grid `orders_*`/`gmv_*`/`unique_dp_*`,
  `views_*`, `orders_*d`, `population_r0`/`pedestrian_traffic_index_r0`, `report_date`,
  POI `*_r{0..5}` и т.д.).
- **Зачем именно ADD COLUMN, а не пересоздание:** старые модельные колонки остаются нетронутыми,
  их значения и downstream-контракт сохраняются. Модельные имена (`population`, `users_orders_r4_30d`,
  `atms_rad_1` и др.) продолжают жить как копии поверх сырых колонок — их переименование потребовало бы
  drop + пересоздание таблицы (destructive-миграции CI запрещает), поэтому расширение сделано только
  добавлением. Миграция идемпотентна: на свежей таблице из `create_table.sql` — no-op, на старой
  52-колоночной — дозаливает недостающие колонки.

## Владелец / алерты

`table.meta.team = team:operations`, alerts `operations-analytics`, severity P3, webhook `oncall_webhook_operations`.
