# Руководство по ML Feature Platform

Этот документ описывает, как работать с `ml-feature-platform`: создавать признаки, проверять существующие контракты, пользоваться MCP для неочевидных источников, менять схемы и публиковать признаки в сервис ранжирования.

Документ не является каталогом всех признаков. Актуальные факты по конкретной таблице всегда берутся из `layers/**`: `config.yaml`, `migrations/*.sql`, `job/*.py`, `dag.py` и `README.md`.

## 1. Что такое сущность платформы

Сущность платформы - это версия пайплайна, обычно каталог вида:

```text
layers/<layer>/<entity_name>/v1
```

Внутри лежит полный контракт:

- `config.yaml` - имя таблицы, catalog/schema/name, primary key, owner metadata;
- `migrations/create_table.sql` и дополнительные миграции - схема Iceberg-таблицы;
- `job/getting_*.py` - PySpark-логика расчета;
- `entrypoints/*.py` - точка запуска Spark job;
- `dag.py` - Airflow orchestration;
- `config/fetch_*.yaml` - SparkApplication template;
- `config/resources.yaml` - ресурсы Spark;
- `README.md` - описание назначения, источников, grain и формул.

## 2. Silver и gold

`silver` - слой для переиспользуемых агрегатов и промежуточных таблиц. Сюда стоит класть результат, если он:

- нормализует внешний источник;
- переиспользуется несколькими `gold`-таблицами;
- содержит дневные агрегаты, из которых потом строятся разные окна;
- еще не является финальным feature contract для модели.

`gold` - слой финальных признаков. Сюда стоит класть результат, если он:

- уже имеет модельный entity grain, например `date,sku_group_id` или `date,query,sku_group_id`;
- содержит колонки, которые можно отдавать модели или downstream-сервису;
- может быть подключен к ranking upload;
- имеет стабильную семантику окон, фильтров, null handling и формул.

Пример: дневные заказы по `query,sku_group_id` удобнее держать в `silver`, а сглаженные conversion-признаки по окнам 7/14/30 дней - в `gold`.

## 3. Как понять, какие признаки уже есть

Перед созданием новой фичи надо сделать duplicate check.

Минимальный набор команд:

```bash
rg -n "<feature_name_or_close_variant>" layers upload scripts docs
rg -n "<source_table_or_filter_value>" layers/**/job layers/**/README.md
rg -n "<column_name>" layers/**/migrations upload/ranking_features/v1
find layers -path '*/config.yaml' -print | sort
```

Что проверять:

- есть ли уже колонка с таким или похожим названием;
- совпадает ли grain;
- совпадают ли окна и включение/исключение `{{ ds }}`;
- совпадают ли фильтры и source semantics;
- как обрабатываются null и zero denominator;
- используется ли таблица в `upload/ranking_features/v1/config.yaml`;
- есть ли downstream jobs, которые читают эту таблицу.

Если похожая фича уже есть, не надо делать дубль автоматически. Надо описать отличие и спросить, действительно ли нужна новая фича.

## 4. Когда использовать MCP-коннектор

Репозиторий - первый источник правды. Но он не всегда отвечает на вопросы о внешних таблицах и бизнес-кодах.

MCP-коннектор Trino или ClickHouse нужен, если:

- таблица читается в job, но не объявлена в `layers/**/config.yaml`;
- непонятна схема внешней таблицы;
- нужно проверить допустимые значения поля вроде `widget_space_name`, `widget_section_name`, `status`, `source`, `space`;
- пользователь просит использовать источник, которого нет в репозитории;
- по коду нашлась похожая таблица, но не доказано, что она подходит для новой фичи.

Важно: найденная таблица, колонка или литерал в коде не являются доказательством source contract. Если в текущем контексте нет явного подтверждения, надо спросить:

```text
В репозитории есть похожий источник `iceberg.silver.order_items_attribution`, но контракт значения `widget_space_name = 'CART'` здесь не документирован. Подтверди, что это правильный источник, или разреши проверить значения и схему через MCP/Trino?
```

Если пользователь разрешил MCP, запрашивайте минимум:

- schema/columns;
- несколько sample rows;
- distinct values для спорного enum/filter поля;
- freshness/partition поле, если оно нужно для расписания.

## 5. Пример: пользователь прислал SQL

Запрос:

```text
Создай признак `sku_group_orders_7d` по SQL ниже:

SELECT
    sku_group_id,
    COUNT(DISTINCT order_id) AS sku_group_orders_7d
FROM iceberg.silver.order_items
WHERE issued_at >= DATE '{{ ds }}' - INTERVAL 6 DAY
  AND issued_at < DATE '{{ next_ds }}'
  AND order_item_status = 'COMPLETED'
GROUP BY sku_group_id
```

Что должен сделать агент:

1. Проверить, есть ли уже такой признак или близкий аналог.
2. Уточнить grain: нужен ли `date,sku_group_id`.
3. Уточнить семантику окна: `{{ ds }}` включен, потому что верхняя граница `< {{ next_ds }}`.
4. Уточнить источник: `order_items` - внешний upstream или уже есть silver-адаптер в Feature Platform?
5. Уточнить ownership и alerts для новой сущности.
6. Предложить слой:
   - если это переиспользуемый дневной агрегат, возможно `silver`;
   - если это финальная модельная фича, `gold`.
7. После подтверждения создать DDL, PySpark job, config, DAG, README и тестово прогнать локальные проверки.

## 6. Пример: создать на основе существующего silver/gold

Если новая фича строится из существующей таблицы Feature Platform, сначала найдите ее владельца и контракт:

```bash
rg -n "feature_platform_sku_group_orders" layers
```

Потом проверьте:

- `layers/**/config.yaml` - primary key и слой;
- `migrations/create_table.sql` - есть ли нужные колонки;
- `job/*.py` - как считаются исходные метрики;
- `README.md` - описаны ли caveats.

Если источник - feature-platform таблица, downstream DAG должен ждать ее dbt DQ DAG, а не Spark DAG записи таблицы.

Пример DQ DAG id:

```text
dbt.source.trino.ml_feature_platform_<schema>.<table_name>.dq
```

## 7. Пример: создать с MCP-коннекшеном

Сценарий: пользователь просит признак по placement, которого нет в README.

Правильный ход:

```text
Я нашел в коде таблицу `iceberg.silver.order_items_attribution`, но репозиторий не подтверждает, какие значения `widget_space_name` соответствуют нужному placement. Подтверди контракт или разреши проверить через MCP/Trino.
```

После разрешения можно выполнить минимальную проверку:

```sql
SELECT widget_space_name, COUNT(*) AS rows_count
FROM iceberg.silver.order_items_attribution
WHERE generated_at >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY widget_space_name
ORDER BY rows_count DESC
LIMIT 50
```

Или проверить схему:

```sql
DESCRIBE iceberg.silver.order_items_attribution
```

После этого в финальном описании надо разделить:

- что подтверждено репозиторием;
- что подтверждено MCP;
- какие assumptions остались.

## 8. Пример вопрос/ответ: какие признаки собираются для отзывов?

Вопрос:

```text
Какие признаки сейчас собираются для отзывов?
```

Ответ строится из файлов `layers/gold/feedback_product_id/v1` и `layers/gold/feedback_sku_group_id/v1`.

Пример ответа:

```text
Для отзывов есть два gold-пайплайна:

1. `iceberg.gold.feature_platform_product_feedback_base_stats`
   Grain: `date,product_id`.

2. `iceberg.gold.feature_platform_sku_group_feedback_base_stats`
   Grain: `date,sku_group_id`.

Оба читают опубликованные отзывы из `iceberg.silver_bxappdb2_foodback.public_feedback`, присоединяют `iceberg.silver.sku`, берут историю до начала расчетного `ds` и считают:

- `product_rating`;
- `bad_reviews_count`;
- `good_reviews_count`;
- `total_reviews_count`;
- `reviews_mark_one_count` ... `reviews_mark_five_count`;
- `total_reviews_with_text`;
- `ratio_reviews_mark_one` ... `ratio_reviews_mark_five`;
- `ratio_reviews_bad`;
- `ratio_reviews_good`.

Для `sku_group_id` версия агрегирует отзывы на уровень `sku_group_id`; для `product_id` - на уровень `product_id`.
```

## 9. Пример вопрос/ответ: как собирается сервис ранжирования?

Вопрос:

```text
Как собирается загрузка признаков в сервис ранжирования?
```

Пример ответа:

```text
Загрузка описана в `upload/ranking_features/v1`.

Основной конфиг - `upload/ranking_features/v1/config.yaml`. В нем перечислены feature groups. Каждая group читает одну gold-таблицу, например `feature_platform_sku_group_feedback_base_stats`, и отправляет упорядоченный список колонок в Kafka topic `ranking.features.updates`.

Перед чтением таблицы upload DAG ждет dbt DQ DAG исходной таблицы. Затем job читает партицию за `{{ ds }}`, строит protobuf `FeaturesUpdate` через `ranking-python-client` и пишет сообщения в Kafka.

Порядок feature groups и размеры в serving contract лежат в `upload/ranking_features/v1/ranking_service_input.yaml`. Порядок важен: в сервис отправляются значения, а не имена колонок.

CI проверяет конфиг через `scripts/validate_ranking_upload_configs.py`: source table должна быть `gold`, все колонки должны существовать в migrations, primary key должен содержать `date`, а entity keys должны быть поддержаны upload job.
```

## 10. Правила именования таблиц и признаков

Таблицы:

- имя таблицы начинается с `feature_platform_`;
- дальше идет сущность и смысл: `sku_group`, `query_skg`, `product`, `price`, `feedback`;
- имя должно отражать grain и предметную область;
- слой берется из `config.yaml`: `silver` или `gold`;
- daily/hourly таблицы обычно имеют `date` в primary key.

Примеры:

```text
feature_platform_sku_group_orders
feature_platform_sku_group_price_features
feature_platform_sku_group_feedback_base_stats
feature_platform_query_skg_pairwise_features_legacy
```

Признаки:

- используйте snake_case;
- название должно отражать сущность, действие, окно и формулу, если это важно;
- окна лучше писать явно: `_7d`, `_14d`, `_30d` или `_7`, `_14`, если такой стиль уже используется в таблице;
- ratio/conversion признаки должны иметь понятный numerator и denominator;
- не меняйте смысл существующей колонки без миграции и проверки downstream.

Примеры:

```text
product_rating
total_reviews_count
ratio_reviews_good
smooth_conv_imp2order_7
query_skg_conv_imp2atc_30
```

Не используйте слишком общие имена вроде `score`, `ratio`, `count`, если из имени непонятны grain, окно и источник.

## 11. Как создать новую таблицу

Пошагово:

1. Сформулируйте контракт:
   - зачем нужна таблица;
   - layer: `silver` или `gold`;
   - grain и primary key;
   - источники и join keys;
   - date/window boundaries;
   - фильтры;
   - null/zero behavior;
   - ownership и alerts;
   - нужен ли ranking upload.

2. Проведите duplicate check:

```bash
rg -n "<feature_or_close_variant>" layers upload scripts docs
rg -n "<source_table_or_filter_value>" layers/**/job layers/**/README.md
rg -n "<column_name>" layers/**/migrations upload/ranking_features/v1
```

3. Если источник или значения фильтров не подтверждены, спросите пользователя или используйте MCP после разрешения.

4. Создайте структуру:

```text
layers/<silver_or_gold>/<entity_name>/v1/
  config.yaml
  dag.py
  config/resources.yaml
  config/fetch_*.yaml
  config/factory.py
  entrypoints/get_*.py
  job/arguments.py
  job/entities.py
  job/getting_*.py
  migrations/create_table.sql
  README.md
```

5. Сначала добавьте `migrations/create_table.sql`.

Пример:

```sql
CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIGINT COMMENT 'ID sku group',
    orders_count_7d BIGINT COMMENT 'Количество заказов за окно [ds - 6, ds]'
)
USING iceberg
COMMENT 'Gold-признак количества заказов на уровне sku_group_id'
PARTITIONED BY (date)
```

6. Реализуйте job. Простой SQL-вариант внутри PySpark:

```python
def build_features(spark, partition_start: str, partition_end: str):
    return spark.sql(
        f"""
WITH params AS (
    SELECT
        TIMESTAMP '{partition_start}' AS ds,
        TIMESTAMP '{partition_end}' AS next_ds
)
SELECT
    CAST(p.ds AS DATE) AS date,
    CAST(s.sku_group_id AS BIGINT) AS sku_group_id,
    CAST(COUNT(DISTINCT oi.order_id) AS BIGINT) AS orders_count_7d
FROM iceberg.silver.order_items oi
CROSS JOIN params p
INNER JOIN iceberg.silver.sku s
    ON s.id = oi.sku_id
WHERE
    oi.issued_at >= p.ds - INTERVAL 6 DAY
    AND oi.issued_at < p.next_ds
    AND oi.order_item_status = 'COMPLETED'
GROUP BY
    p.ds,
    s.sku_group_id
"""
    )
```

7. В `config.yaml` укажите table metadata и owner metadata.

8. В `dag.py` добавьте DQ sensors для feature-platform зависимостей. Для внешних upstream источников используйте контракт команды-владельца источника.

9. В `README.md` опишите:
   - назначение;
   - целевую таблицу;
   - источники;
   - grain;
   - окна;
   - формулы;
   - caveats.

10. Запустите локальные проверки.

## 12. Как удалить таблицу

Удаление таблицы - это lifecycle, а не один `DROP TABLE`.

Порядок:

1. Классифицируйте действие:
   - deprecate only;
   - stop producing;
   - remove from ranking upload;
   - remove from repo ownership;
   - physical drop/archive.

2. Найдите downstream:

```bash
rg -n "<table_name>|<feature_name>" layers upload scripts docs
```

3. Если repo не доказывает отсутствие внешних потребителей, спросите владельцев или предложите MCP/catalog inspection.

4. Если таблица публикуется в ranking service, сначала согласуйте serving compatibility и удалите feature group или колонку из `upload/ranking_features/v1/config.yaml` и `ranking_service_input.yaml`.

5. Для рискованных случаев сначала добавьте deprecation notice в README/config и договоритесь о grace period.

6. Stop producing может означать удаление/паузу DAG или удаление layer directory из repo, но физические Iceberg-данные остаются до отдельного решения.

7. Не добавляйте destructive migrations:

```sql
DROP TABLE ...
DELETE FROM ...
TRUNCATE TABLE ...
```

Обычная migration CI такие операции отвергает. Physical drop/archive должен быть отдельным согласованным runbook.

8. После merge проверьте generated PR в `dbt-trino` и maintenance PR в `DayMarket/pyspark-etl`.

## 13. Дефолтные DQ-тесты

Для repository-managed таблиц dbt source sync создает DQ tests в `dbt-trino`.

По умолчанию:

- `dbt_utils.unique_combination_of_columns` по всем колонкам из `table.primary_key`;
- `not_null` для каждой primary key колонки;
- если в primary key есть `date`, добавляется freshness:
  - `loaded_at_field = CAST(date AS timestamp) + INTERVAL '1' DAY`;
  - `error_after: count: 2, period: day`;
- если есть `date`, добавляется row-count test за предыдущий день с `min_rows: 0`;
- если есть `date`, добавляется growth-limit test за предыдущий день с `max_growth_ratio: 0.2`.

Дополнительные DQ-тесты стоит предлагать только когда они являются частью feature contract:

- accepted values для enum/status;
- range checks для ratio, probability, rating, price, count;
- non-negative checks;
- consistency checks, например `min <= median <= max`;
- более сильный row-count threshold, если `min_rows: 0` слишком слабый.

Не добавляйте дорогие relationship tests по высококардинальным ключам без явного согласования.

## 14. Какие PR создаются после merge

После merge в `master` CI может создать downstream PR:

- в `DayMarket/dbt-trino` - source definitions и DQ-тесты для новых/измененных repository-managed таблиц;
- в `DayMarket/pyspark-etl` - регистрация Iceberg maintenance для таблиц из `layers/**/config.yaml`.

Что важно:

- эти PR создаются master-side CI, не во время feature-branch стадии;
- ссылки на PR пишутся в CI logs и могут комментироваться в source PR;
- перед merge downstream PR надо проверить, что добавлены только таблицы, созданные `ml-feature-platform`;
- maintenance sync не должен добавлять внешние dependency tables вроде `iceberg.silver.order_items`;
- removal из maintenance требует ручного review.

## 15. Как добавить новую колонку в таблицу

Сначала убедитесь, что изменение схемы безопасно.

Порядок:

1. Найдите downstream:

```bash
rg -n "<table_name>|<old_column>|<new_column>" layers upload scripts docs
```

2. Если таблица используется downstream, проверьте:
   - не ожидается ли фиксированный список колонок;
   - не используется ли `select *`;
   - не публикуется ли таблица в ranking upload;
   - не сломает ли новая колонка порядок feature vector.

3. Если есть зависимые downstream таблицы или сервисы, сначала согласуйте контракт изменения. Не добавляйте колонку молча.

4. Обновите `migrations/create_table.sql`, чтобы новые окружения создавались сразу с новой колонкой.

5. Добавьте отдельную idempotent migration для существующих окружений:

```sql
ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS new_feature DOUBLE COMMENT 'Описание новой фичи'
```

6. Обновите PySpark job: колонка должна попадать в финальный `select` и запись.

7. Обновите README таблицы.

8. Если колонка должна публиковаться в ranking service:
   - добавьте ее в `upload/ranking_features/v1/config.yaml`;
   - обновите `ranking_service_input.yaml`;
   - сохраните правильный порядок values.

9. Запустите проверки.

## 16. Как работать с ranking upload

Ranking upload находится в `upload/ranking_features/v1`.

Основные правила:

- source table должна быть repository-managed `gold`-таблицей;
- одна feature group читает ровно одну source table;
- `features` - упорядоченный список колонок, порядок является serving contract;
- feature names не отправляются в сервис, отправляются только значения;
- `source.limit` запрещен в production;
- `log1p_features` можно использовать только для колонок из `features`;
- primary key source table должен содержать `date`;
- entity keys = primary key без `date`;
- entity keys должны поддерживаться upload job.

Поддерживаемые entity keys сейчас:

- `sku_group_id`;
- `query`;
- `account_id`;
- `query,sku_group_id`;
- `category_id,sku_group_id`;
- `account_id,category_id`.

Процесс добавления feature group:

1. Найдите `gold`-таблицу в `layers/**/config.yaml`.
2. Проверьте, что нужные колонки есть в migrations.
3. Добавьте блок в `upload/ranking_features/v1/config.yaml`.
4. Обновите `upload/ranking_features/v1/ranking_service_input.yaml`: name, schema и size.
5. Проверьте `source.dq_execution_delta_minutes`.
6. Запустите:

```bash
python3 scripts/validate_ranking_upload_configs.py
```

## 17. Другие важные особенности

- `{{ ds }}` - это partition date, но включение или исключение `ds` зависит от конкретной фичи. Всегда проверяйте job и README.
- Для feature-platform зависимостей downstream DAG должен ждать dbt DQ DAG, а не Spark DAG.
- Для внешних источников используйте DQ/source contract команды-владельца.
- Не прячьте source table names в неочевидных константах: lineage должен читаться из job.
- Не добавляйте custom Spark image для обычных code/config/SQL changes. Используйте default Spark image и `git-sync`.
- Если нужна новая Python-библиотека, truststore или бинарь, сначала объясните, почему `git-sync` не подходит, и только потом меняйте image.
- Не обновляйте `AGENTS.md` при добавлении каждой новой фичи. Детали фичи должны жить в README слоя, migration, config, DAG и job.
- Перед финалом всегда упоминайте, какие проверки были запущены и что не удалось проверить локально.

## 18. Минимальный шаблон финального ответа по новой таблице

```text
Создана таблица `iceberg.gold.feature_platform_example`.

Grain: `date,sku_group_id`.
Источники: `iceberg.silver.example_source`, join по `sku_id`.
Окна: `7d = [ds - 6, ds]`, `ds` включен.
Фичи: `example_feature_7d`.
DQ: дефолтные dbt source tests по primary key и freshness.
Runtime: default Spark image + git-sync.
Downstream: ranking upload не добавлялся.

Проверки:
- python3 ci_test/test_script.py
- python3 ci_test/test_sync_dbt_sources.py
- python3 ci_test/test_sync_iceberg_maintenance.py
- python3 scripts/validate_ranking_upload_configs.py
- git diff --check
```
