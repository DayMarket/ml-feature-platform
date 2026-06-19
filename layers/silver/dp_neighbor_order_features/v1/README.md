# silver.feature_platform_dp_neighbor_order_features

Соседские (конкурентные) заказные/GMV-агрегаты ПВЗ и дистанции до ближайших точек по гексагонам H3 (res 9).

## Грейн / ключ
`date, h3_index`. `date` — дата расчёта (data_interval_end в UTC). `h3_index` — H3-индекс центрального гексагона.

## Источники (внешние, ClickHouse)
- `marts.order_items` — заказы (orders_fact, gmv через `daily_uzs_to_usd`), статусы COMPLETED/RETURNED.
- `dict.delivery_point` — координаты/тип ПВЗ (DELIVERY_POINT/FRANCHISE -> is_dp, UZ_POST -> is_inshop).

## Логика
Для каждого гексагона в радиусе 5 колец вокруг любого ПВЗ ищутся соседние ПВЗ (открытые до calc_date),
считаются `geoDistance` до ближайшего ПВЗ/in-shop и оконные суммы заказов/GMV и число уникальных ПВЗ.
**Расширенная сетка**: rings 0..5 x окна 30/60/90 для `orders_*`, `gmv_*`, `unique_dp_*`
(в продакшен-запросе материализовалась лишь часть комбинаций). Окно — заказы за N дней до `date`.
Дистанции без соседей = 10000 м.

## Рантайм

ClickHouse-source пайплайн (Airflow/Python + `pyiceberg`), **не** Spark. Чтение из ClickHouse через
connection `clickhouse_dwh_team_logistics`, запись в Iceberg через общий модуль
`layers/_common/clickhouse_iceberg.py`. Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.
Каталог Iceberg (Hive metastore, warehouse, YC S3) настроен идентично Spark-шаблону
`config/spark/layer_spark_application.yaml`; ключи S3 берутся из connection `spark_ycs_connection`.

Оркестрация — DAG `location_forecast_features_dag` (`layers/gold/location_h3_forecast_features/v1/dag.py`),
ежедневно 00:00 UTC. Запись идемпотентна: партиция `date` перезаписывается целиком (`overwrite` по фильтру `date`).

## Владелец / алерты

`table.meta.team = team:operations`, alerts `operations-analytics`, severity P3, webhook `oncall_webhook_operations`.
