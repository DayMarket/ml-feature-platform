# iceberg.silver.feature_platform_geo_yandex_poi_features

Количество POI Яндекса по бизнес-категориям и кольцам H3 (res 9).

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_geo_yandex_poi_features`.
- DAG: `ml-feature-platform.layers.silver.geo_yandex_poi_features` (`layers/silver/geo_yandex_poi_features/v1/dag.py`).
- Расписание: ежедневно в 00:00 UTC, `start_date=2026-06-19T00:00:00Z`.

## Грейн / ключ
`date, h3_index`.

## Источник (внешний, ClickHouse)
- `silver.organizations_yandex` — организации Яндекса (последний снимок по `inserted_at`), поле `category_sub`.

## Логика
Категории (бизнес-enum источника, сохранён как в исходном контракте): Банкоматы, Банки, Торговые точки,
Автосалоны/Автосервисы, Смешанные товары, Быстрое питание/Кофейни, Пекарни. **Расширенная сетка**:
каждая категория x кольца 0..5 (`<cat>_r{0..5}`), тогда как продакшен материализовал по одному радиусу на категорию.

## Рантайм

ClickHouse-source пайплайн (Airflow/Python + `pyiceberg`), **не** Spark. Чтение из ClickHouse через
connection `clickhouse_dwh_team_logistics`, запись выполняет entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.
Каталог Iceberg (Hive metastore, warehouse, YC S3) настроен идентично Spark-шаблону
`config/spark/layer_spark_application.yaml`; ключи S3 берутся из connection `spark_ycs_connection`.

Перед запросом к ClickHouse DAG проверяет существование таблицы через PyIceberg с идентификатором
`("silver", "feature_platform_geo_yandex_poi_features")`. Запись идемпотентна: партиция `date` перезаписывается целиком
(`overwrite` по фильтру `date`).

## Владелец / алерты

`table.meta.team = team:operations`, alerts `operations-analytics`, severity P3, webhook `oncall_webhook_operations`.
