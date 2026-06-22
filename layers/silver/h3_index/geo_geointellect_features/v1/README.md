# iceberg.silver.feature_platform_geo_geointellect_features

Демография (население) и индекс пешеходного трафика по кольцам H3 (res 9) и родительским уровням.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_geo_geointellect_features`.
- DAG: `feature-platform.layers.silver.h3_index.geo_geointellect_features` (`layers/silver/h3_index/geo_geointellect_features/v1/dag.py`).
- Групповой тег Airflow: `location-h3-forecast`.
- Расписание: ежедневно в 00:00 UTC, `start_date=2026-06-19T00:00:00Z`.

## Грейн / ключ
`date, h3_index`. Дополнительно: `h3_string`, `region` (по `geopoint2region`).

## Источник (внешний, ClickHouse)
- `silver.h3_l9_geointellect` — `population`, `pedestrian_traffic_index` по гексагонам res 9.

## Логика
База — гексагоны с `population>0 OR pedestrian_traffic_index>0`. Кумулятивные суммы по кольцам 0..5
(`population_r{0..5}`, `pedestrian_traffic_index_r{0..5}`) плюс роллапы по родительским гексагонам уровней 5..8.
Признак `traffic_ring_1_2` (кольцо ровно 2) выводится в gold как `r2 - r1`.

## Рантайм

ClickHouse-source пайплайн (Airflow/Python + `pyiceberg`), **не** Spark. Чтение из ClickHouse через
connection `clickhouse_dwh_team_logistics`, запись выполняет entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.
Каталог Iceberg (Hive metastore, warehouse, YC S3) настроен идентично Spark-шаблону
`config/spark/layer_spark_application.yaml`; ключи S3 берутся из connection `spark_ycs_connection`.

Перед запросом к ClickHouse DAG проверяет существование таблицы через PyIceberg с идентификатором
`("silver", "feature_platform_geo_geointellect_features")`. Запись идемпотентна: партиция `date` перезаписывается целиком
(`overwrite` по фильтру `date`).

## Владелец / алерты

`table.meta.team = team:operations`, alerts `operations-analytics`, severity P3, webhook `oncall_webhook_operations`.
