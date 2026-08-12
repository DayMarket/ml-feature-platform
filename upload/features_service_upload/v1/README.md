# Загрузка признаков в сервис ранжирования

Один DAG загружает feature groups из таблиц feature platform в Kafka-топик сервиса ранжирования. Внутри DAG upload разбивается на независимые компоненты по модельному manifest-у.

Основной конфиг: `config.yaml`.

Каждая feature group должна читать ровно одну исходную таблицу. Таблицы не объединяются внутри upload job: если признаки находятся в разных источниках, для них создаются отдельные feature groups с отдельными именами в сервисе ранжирования.

`feature_groups` — каталог того, что платформа умеет публиковать. Для каждой feature group обязательны:

- `source.schema` и `source.table` — исходная таблица в репозитории;
- `name` — имя feature group в сервисе ранжирования;
- `features` — упорядоченный список колонок исходной таблицы. Порядок важен, потому что в protobuf отправляется массив значений без имен.

Catalog, колонка даты и ключи сущности автоматически берутся из `layers/**/config.yaml`: `entity_keys` равны `primary_key` без колонки `date`.

Опциональные поля:

- `log1p_features`: список признаков, к которым перед отправкой применяется `log1p`.
- `source.limit`: ограничение числа строк после фильтрации партиции. Поле предназначено только для тестовой проверки загрузки.
- `source.dependency_dag_id`: DAG, который должен успешно завершиться перед загрузкой source-таблицы.
- `source.dependency_execution_delta_minutes`: разница между расписанием upload DAG и upstream DAG. Например, upload в `04:00 UTC` и upstream в `03:00 UTC` дают `60`.

На текущей итерации daily upload напрямую ждёт gold producer DAG-и, потому что общие dbt source/DQ DAG-и запускаются в `01:00 UTC`, раньше обновления gold-таблиц. Upload стартует в `04:00 UTC`: зависимости на producer в `02:00`, `03:00` и `03:10 UTC` используют delta `120`, `60` и `50` минут соответственно. После переноса DQ DAG на время после producer эту временную схему следует вернуть к ожиданию DQ.

Upload DAG имеет `start_date=2026-07-23T00:00:00+00:00`, расписание интерпретируется в UTC.

Production-конфиг не должен содержать `source.limit`, чтобы DAG загружал полные партиции всех feature groups.

Пример:

```json
{
  "source": {
    "schema": "gold",
    "table": "feature_platform_sku_group_price_features"
  },
  "name": "fs_search_skg_prices_v1",
  "features": ["sell_price_eod", "fraq_discount"]
}
```

Нельзя использовать одно и то же `name` для нескольких неполных наборов признаков из разных таблиц: сервис получает массив значений без имен и ожидает единый согласованный контракт feature group.

Порядок feature groups для текущей конфигурации сервиса ранжирования приведен в `ranking_service_input.yaml`. Этот manifest не обязан перечислять новые параллельно публикуемые версии feature group до их подключения на стороне новой модели.

`models` описывает, какие признаки из каких feature groups использует конкретная модель. Feature всегда указывается внутри своей feature group:

```json
{
  "name": "search_ranking_main",
  "feature_groups": [
    {
      "name": "fs_search_skg_price_features_v1",
      "features": ["sell_price_eod", "abs_discount"]
    }
  ]
}
```

Если новая модель использует только свои feature groups, DAG создаст для нее независимую `TaskGroup` с upload task и своими DQ-сенсорами. Если новая модель использует хотя бы одну общую feature group с другой моделью, эти модели попадут в одну `TaskGroup`. Внутри нее upload task дождется общего набора DQ и загрузит union признаков по shared feature groups. Разные `TaskGroup` не зависят друг от друга. Если новая модель использует только уже существующие feature groups и признаки, новые feature groups создавать не нужно.

Новая feature group нужна только для нового serving-контракта группы: другой `name`, другой source/entity contract или новый namespace/версия публикации. Разные модели могут брать разные subset-ы признаков из одной feature group.

Группа `fs_search_query_skg_atc_order_features_v2` читает 41 query/SKU-group признак из `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features_v2`. Группа `fs_search_query_skg_atc_order_features_v3` публикуется параллельно из той же таблицы: она сохраняет порядок 41 признака `v2`, добавляя в начало `query_skg_conv_imp2atc_90` и `query_skg_conv_imp2order_90`. Остальные группы читают признаки на grain `sku_group_id` или `query` из своих gold-таблиц, перечисленных в `config.yaml`.

CI запускает `scripts/validate_ranking_upload_configs.py`. Проверка находит исходную таблицу по `layers/**/config.yaml`, получает `primary_key`, читает колонки из миграций и завершает сборку с ошибкой, если ключи или признаки отсутствуют.

Тип protobuf-сообщения выбирается автоматически по ключам сущности. Сейчас поддерживаются:

- `sku_group_id`;
- `query`;
- `account_id`;
- `query,sku_group_id`;
- `category_id,sku_group_id`;
- `account_id,category_id`.

DAG ждёт gold producer DAG каждой зависимой source-таблицы, затем читает партицию за `{{ ds }}`, сериализует `FeaturesUpdate` через `ranking-python-client` и пишет сообщения в `ranking.features.updates`.

Kafka key строится как `feature_group_name|entity_keys...`. Это важно, чтобы разные feature groups для одного `sku_group_id` не конфликтовали в compacted topic или в downstream-дедупликации по key.

Код job доставляется через `git-sync`. Отдельный Docker image содержит только runtime-зависимость `ranking-python-client` и Kafka truststore, поэтому изменения job или `config.yaml` не требуют пересборки image.
