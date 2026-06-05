# Загрузка признаков в сервис ранжирования

Один DAG загружает несколько feature groups из таблиц feature platform в Kafka-топик сервиса ранжирования.

Основной конфиг: `config.yaml`.

Каждая feature group должна читать ровно одну исходную таблицу. Таблицы не объединяются внутри upload job: если признаки находятся в разных источниках, для них создаются отдельные feature groups с отдельными именами в сервисе ранжирования.

Для каждой feature group обязательны:

- `source.schema` и `source.table` — исходная таблица в репозитории;
- `name` — имя feature group в сервисе ранжирования;
- `features` — упорядоченный список колонок исходной таблицы. Порядок важен, потому что в protobuf отправляется массив значений без имен.

Catalog, колонка даты и ключи сущности автоматически берутся из `layers/**/config.yaml`: `entity_keys` равны `primary_key` без колонки `date`.

Опциональные поля:

- `log1p_features`: список признаков, к которым перед отправкой применяется `log1p`.
- `source.limit`: ограничение числа строк после фильтрации партиции. Поле предназначено только для тестовой проверки загрузки.
- `source.dq_execution_delta_minutes`: разница между расписанием upload DAG и DQ DAG исходной таблицы. Например, upload в `04:00` и source/DQ в `03:00` дают `60`.

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

Порядок feature groups для конфигурации сервиса ранжирования приведен в `ranking_service_input.yaml`. Он сохраняет порядок значений предыдущего `fs_search_skg_v2`, хотя признаки теперь загружаются из отдельных источников.

CI запускает `scripts/validate_ranking_upload_configs.py`. Проверка находит исходную таблицу по `layers/**/config.yaml`, получает `primary_key`, читает колонки из миграций и завершает сборку с ошибкой, если ключи или признаки отсутствуют.

Тип protobuf-сообщения выбирается автоматически по ключам сущности. Сейчас поддерживаются:

- `sku_group_id`;
- `query`;
- `account_id`;
- `query,sku_group_id`;
- `category_id,sku_group_id`;
- `account_id,category_id`.

DAG ждет DQ DAG каждой зависимой source-таблицы, затем читает партицию за `{{ ds }}`, сериализует `FeaturesUpdate` через `ranking-python-client` и пишет сообщения в `ranking.features.updates`. Текущий smoke-test конфиг содержит `source.limit: 5`, поэтому ожидаемый объем - до 5 Kafka records на feature group.

Kafka key строится как `feature_group_name|entity_keys...`. Это важно, чтобы разные feature groups для одного `sku_group_id` не конфликтовали в compacted topic или в downstream-дедупликации по key.

Код job доставляется через `git-sync`. Отдельный Docker image содержит только runtime-зависимость `ranking-python-client` и Kafka truststore, поэтому изменения job или `config.yaml` не требуют пересборки image.
