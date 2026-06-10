# Руководство по ML Feature Platform

Этот документ описывает, как работать с `ml-feature-platform`: создавать признаки, проверять существующие контракты, выбирать источники, менять схемы и публиковать признаки в сервис ранжирования.

Платформа не ограничена поиском. В ней можно собирать признаки для любых продуктовых ML-сценариев: поиск, рекомендации, открытие ПВЗ, матчинг, ранжирование, персонализация и другие задачи.

Важное текущее ограничение: все источники должны быть доступны как Iceberg-таблицы. Если нужных данных еще нет в Feature Platform, источник подбирается среди Iceberg-таблиц через Trino или ClickHouse, затем в репозитории создается слой трансформации.

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

- нормализует Iceberg-источник;
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

Перед созданием новой фичи надо сделать duplicate check. ML-инженеру не нужно вручную обходить репозиторий: можно просто спросить агента.

Вопросы могут быть любого такого характера:

```text
Есть ли уже признаки продаж по `sku_group_id` за 7 дней?
```

```text
Проверь, нет ли уже фичи `product_rating` и где она используется.
```

```text
Какие признаки сейчас собираются для отзывов и в какие таблицы они пишутся?
```

```text
Как считается конкретная фича `query_skg_conv_imp2atc_30`: источник, окно, фильтры, null handling?
```

В ответе агент должен вернуть не список файлов, а понятный вывод:

- нашлась ли такая же фича;
- в какой таблице она лежит и какой у нее grain;
- какие окна и date boundaries используются, включая включение/исключение `{{ ds }}`;
- какие источники, join keys и фильтры участвуют в расчете;
- как обрабатываются `NULL`, нулевые знаменатели и пустые окна;
- публикуется ли фича в ranking upload;
- есть ли похожие фичи и чем они отличаются.

Пример ответа:

```text
Похожая фича уже есть: `product_rating` в `iceberg.gold.feature_platform_product_feedback_base_stats`.
Grain: `date,product_id`.
Источник: опубликованные отзывы из Iceberg-таблицы feedback и справочник sku.
Семантика: рейтинг считается по истории отзывов до расчетной даты; отдельно считаются bucket counts по оценкам 1..5 и ratio good/bad.
В ranking upload эта таблица используется, поэтому изменение колонки будет serving contract change.
```

При проверке агент смотрит:

- есть ли уже колонка с таким или похожим названием;
- совпадает ли grain;
- совпадают ли окна и включение/исключение `{{ ds }}`;
- совпадают ли фильтры и source semantics;
- как обрабатываются null и zero denominator;
- используется ли таблица в `upload/ranking_features/v1/config.yaml`;
- есть ли downstream jobs, которые читают эту таблицу.

Если похожая фича уже есть, не надо делать дубль автоматически. Надо описать отличие и спросить, действительно ли нужна новая фича.

## 4. Если нужного источника нет в Feature Platform

Сначала стоит проверить, нет ли нужных данных уже внутри Feature Platform:

- в `layers/silver` - переиспользуемые агрегаты и адаптеры источников;
- в `layers/gold` - готовые модельные признаки.

Если подходящей таблицы нет, источник выбирается среди Iceberg-таблиц. Для этого используется Trino или ClickHouse: можно посмотреть схему, партиции, свежесть данных, примеры строк и значения полей вроде `widget_space_name`, `widget_section_name`, `status`, `source`, `space`.

Пример запроса:

```text
В Feature Platform нет готовой таблицы для этого признака. Подбери Iceberg-источник через Trino: проверь схему, партиции и возможные значения `widget_space_name`.
```

После выбора источника нужно зафиксировать:

- полное имя Iceberg-таблицы;
- владельца источника или команду, которая отвечает за данные;
- freshness/DQ contract источника;
- ключи join;
- поля партиционирования и даты;
- бизнес-смысл фильтров и enum values.

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
4. Уточнить источник: есть ли уже silver-адаптер в Feature Platform или нужно выбрать Iceberg-источник через Trino/ClickHouse.
5. Уточнить ownership и alerts для новой сущности.
6. Предложить слой:
   - если это переиспользуемый дневной агрегат, возможно `silver`;
   - если это финальная модельная фича, `gold`.
7. После подтверждения создать DDL, PySpark job, config, DAG, README и тестово прогнать локальные проверки.

## 6. Пример: создать на основе существующего silver/gold

Если новая фича строится из существующей таблицы Feature Platform, попросите агента найти ее владельца и контракт. Он проверит:

- `layers/**/config.yaml` - primary key и слой;
- `migrations/create_table.sql` - есть ли нужные колонки;
- `job/*.py` - как считаются исходные метрики;
- `README.md` - описаны ли caveats.

Если источник - feature-platform таблица, downstream DAG должен ждать ее dbt DQ DAG, а не Spark DAG записи таблицы.

Пример DQ DAG id:

```text
dbt.source.trino.ml_feature_platform_<schema>.<table_name>.dq
```

## 7. Пример: создать с Trino/ClickHouse-источником

Сценарий: пользователь просит признак по placement, которого нет в готовых Feature Platform таблицах.

Пример запроса:

```text
Собери признак продаж из корзины. Если готового источника в Feature Platform нет, проверь через Trino, какая Iceberg-таблица содержит атрибуцию и какие значения есть в `widget_space_name`.
```

Пример SQL-проверки источника:

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

После проверки источник фиксируется в контракте новой таблицы:

- source table: `iceberg.silver.order_items_attribution`;
- join key: например `order_item_id`;
- filter: например `widget_space_name = 'CART'`;
- дата/партиция: например `generated_at`;
- downstream DQ/freshness ожидания.

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

CI проверяет, что source table для upload находится в `gold`, все перечисленные признаки существуют в migrations, primary key содержит `date`, а entity keys поддержаны upload job.
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

Пользователь может описать задачу обычным текстом. Чем больше контракта есть в первом сообщении, тем меньше уточнений потребуется.

Пример хорошего запроса:

```text
Создай gold-таблицу с признаками количества заказов по `sku_group_id` за 7, 14 и 28 дней.
Grain: `date,sku_group_id`.
`{{ ds }}` включаем, это Airflow macro за вчерашний день.
Продажи считаем как `COUNT(DISTINCT order_id)`.
Фильтр атрибуции: `widget_space_name = 'CART'`.
Источник можно подобрать из Iceberg через Trino, если готовой Feature Platform таблицы нет.
```

Обычно агент уточнит:

- это финальная `gold`-фича или нужен переиспользуемый `silver`-агрегат;
- какие source tables считать контрактными и нужно ли проверять их через Trino/ClickHouse;
- какие join keys связывают источник продаж с целевой сущностью;
- какие date boundaries у окон: включен ли `{{ ds }}`, какая верхняя граница, какие timestamp/date поля использовать;
- какие статусы заказов считать продажей и как учитывать отмены/возвраты;
- что делать с отсутствующими значениями: не писать строку, писать `0`, оставлять `NULL`;
- нужна ли публикация в ranking upload;
- какие `table.meta.team`, `dag.team`, `alerts.team`, severity и on-call webhook использовать.

После согласования агент проводит duplicate check и кратко возвращает выбранный contract: слой, имя таблицы, primary key, источники, окна, фильтры, DQ и downstream-публикацию. Только после этого создаются файлы.

Если нужна похожая реализация, можно попросить агента использовать MCP-коннектор к Git: посмотреть старые PR, историю изменений или уже удаленные/измененные реализации и собрать новую сущность по проверенному шаблону. Это удобно, когда новая таблица похожа на старую по Airflow/Spark структуре, но отличается источником, grain или набором признаков. В таком случае агент должен явно сказать, какие части он взял из старого примера, а какие поменял под новый contract.

Для новой таблицы обычно создается такая структура кода:

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

Что означает каждая сущность:

- `config.yaml` задает Iceberg catalog/schema/name, primary key и ownership metadata. По нему CI понимает, что таблица принадлежит Feature Platform.
- `migrations/create_table.sql` создает Iceberg-таблицу и комментарии колонок. Эта миграция реально применяется CI после merge в `master`.
- дополнительные migrations нужны для изменений уже существующих таблиц, например добавления новой колонки.
- `job/arguments.py` и `job/entities.py` описывают runtime-аргументы, обычно `partition_start`, `partition_end` и `table_name`.
- `job/getting_*.py` содержит основной PySpark-расчет: чтение источников, join, фильтры, окна, формулы и финальный `select`.
- `entrypoints/*.py` создает `SparkSession`, читает аргументы, вызывает job и корректно закрывает Spark.
- `dag.py` описывает Airflow DAG, расписание, Spark task и DQ/sensor dependencies.
- `config/fetch_*.yaml` - SparkApplication template для запуска в Kubernetes.
- `config/factory.py` подставляет в SparkApplication значения из config, resources, Airflow variables и macros.
- `config/resources.yaml` задает ресурсы driver/executor.
- `README.md` фиксирует человеческий contract: назначение, источники, grain, окна, формулы, caveats и downstream-использование.

Пример DDL для новой таблицы:

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

Пример расчета внутри PySpark job:

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

Финально агент запускает релевантные проверки и сообщает, что проверено локально, а что остается на CI.

## 12. Как удалить таблицу

Удаление таблицы - это lifecycle, а не один `DROP TABLE`.

Пользователь должен написать, что именно хочется сделать с таблицей:

```text
Таблица `iceberg.gold.feature_platform_example` больше не нужна. Проверь потребителей и предложи безопасный план удаления.
```

Если уже известно, что нужно только убрать публикацию в сервис, лучше сказать это явно:

```text
Убери фичу из ranking upload, но саму gold-таблицу пока продолжай считать.
```

Агент сначала определит тип действия: только deprecate, остановить производство новых партиций, убрать из ranking upload, убрать ownership из репозитория или готовить отдельный physical drop/archive. Затем он проверит потребителей в Feature Platform, ranking upload, скриптах синхронизации и документации. Если по репозиторию нельзя доказать, что внешних потребителей нет, агент попросит подтвердить consumer contract или разрешить проверку через каталог/Trino/ClickHouse.

Если таблица участвует в ranking service, сначала согласуется serving compatibility: можно ли удалить feature group или колонку, не ломает ли это порядок feature vector, нужен ли grace period. Для рискованных случаев правильнее сначала добавить deprecation notice, остановить downstream upload, дождаться подтверждения от потребителей и только потом прекращать производство таблицы.

Важно: обычная миграция не должна физически удалять данные. `DROP`, `DELETE` и `TRUNCATE` не добавляются в repository migrations. Физический drop/archive Iceberg-данных должен быть отдельным согласованным runbook. После merge нужно проверить downstream PR в `dbt-trino` и maintenance PR в `DayMarket/pyspark-etl`, потому что удаление source/DQ/maintenance contract требует review.

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

## 14. Что происходит после готового кода

Когда код готов, путь до production выглядит так:

1. Код готов в feature branch: создана или изменена структура слоя, миграции, PySpark job, config, DAG и README.
2. PR мержится в `dev`: должны пройти CI-проверки. На этом этапе side-effecting шаги не должны создавать таблицы, менять dbt-trino, maintenance или Airflow submodule.
3. PR мержится в `master`: CI применяет миграции и создает или обновляет Iceberg-таблицу. Для новых repository-managed таблиц также создаются два downstream PR: в `DayMarket/dbt-trino` и `DayMarket/pyspark-etl`.
4. После master merge надо проверить таблицу в Iceberg: схема, партиция, наличие данных за ожидаемый `ds`, ключи, базовые агрегаты и несколько sanity-check значений.
5. Пока downstream PR не мержатся автоматически, нужно вручную сходить в оба репозитория: `DayMarket/dbt-trino` и `DayMarket/pyspark-etl`. Для них надо запросить review, временно через DE, проверить diff и замержить.
6. После merge downstream PR надо включить основной Airflow DAG и DQ DAG. Основной DAG пишет таблицу, DQ DAG проверяет source contract для downstream-потребителей.
7. После первого успешного запуска надо проверить DQ, свежесть партиции и, если есть ranking upload, что upload DAG дождался DQ и отправил ожидаемый feature group.

Важно: таблица в Iceberg создается не локально и не при merge в `dev`, а master-side CI при merge в `master`.

## 15. Какие PR создаются после merge

После merge в `master` CI может создать downstream PR:

- в `DayMarket/dbt-trino` - source definitions и DQ-тесты для новых/измененных repository-managed таблиц;
- в `DayMarket/pyspark-etl` - регистрация Iceberg maintenance для таблиц из `layers/**/config.yaml`.

Что важно:

- эти PR создаются master-side CI, не во время feature-branch стадии;
- ссылки на PR пишутся в CI logs и могут комментироваться в source PR;
- эти PR пока надо открыть в соответствующих репозиториях и замержить вручную;
- перед merge downstream PR надо проверить, что добавлены только таблицы, созданные `ml-feature-platform`;
- maintenance sync не должен добавлять внешние dependency tables вроде `iceberg.silver.order_items`;
- removal из maintenance требует ручного review.

## 16. Как добавить новую колонку в таблицу

Пользователь может написать короткий запрос:

```text
Добавь колонку `orders_count_28d` в `feature_platform_sku_group_orders`.
Окно 28 дней, `{{ ds }}` включен, считать как `COUNT(DISTINCT order_id)`.
```

Перед изменением агент проверит, безопасна ли схема:

- есть ли downstream-таблицы или сервисы, которые читают эту таблицу;
- используется ли фиксированный список колонок;
- есть ли места с `select *`, где новая колонка может неожиданно уехать дальше;
- публикуется ли таблица в ranking upload;
- изменится ли serving contract или порядок feature vector;
- нет ли уже такой или очень похожей колонки.

Если downstream-зависимости есть, изменение сначала согласуется как contract change. Новую колонку не стоит добавлять молча, если потребитель ожидает фиксированный набор колонок или таблица участвует в feature vector.

После согласования агент обновит `migrations/create_table.sql`, чтобы новые окружения сразу создавались с колонкой, и добавит отдельную idempotent migration для существующих окружений:

```sql
ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS new_feature DOUBLE COMMENT 'Описание новой фичи'
```

Также обновляются PySpark job, README таблицы и, если колонка должна попасть в ranking service, `upload/ranking_features/v1/config.yaml` и `ranking_service_input.yaml`. В конце агент запускает проверки и отдельно сообщает, менялся ли ranking serving contract.

## 17. Как работать с ranking upload

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

Чтобы добавить feature group, пользователь описывает, какую `gold`-таблицу и какие колонки нужно отдавать в ranking service. Агент проверяет, что таблица действительно repository-managed `gold`, что все колонки есть в migrations, что entity keys поддержаны upload job, что `source.dq_execution_delta_minutes` соответствует DQ DAG исходной таблицы, и что порядок колонок согласован с serving contract.

После этого обновляются `upload/ranking_features/v1/config.yaml` и `upload/ranking_features/v1/ranking_service_input.yaml`. Проверка ranking upload подтверждает, что source table, schema, feature list, entity keys и размеры feature groups согласованы между конфигами и миграциями.

## 18. Другие важные особенности

- `{{ ds }}` - это partition date, но включение или исключение `ds` зависит от конкретной фичи. Агент должен сверить это с job и README конкретной таблицы.
- Для feature-platform зависимостей downstream DAG должен ждать dbt DQ DAG, а не Spark DAG.
- Для внешних источников используйте DQ/source contract команды-владельца.
- Не прячьте source table names в неочевидных константах: lineage должен читаться из job.
- Не добавляйте custom Spark image для обычных code/config/SQL changes. Используйте default Spark image и `git-sync`.
- Не обновляйте `AGENTS.md` при добавлении каждой новой фичи. Детали фичи должны жить в README слоя, migration, config, DAG и job.
- Перед финалом всегда упоминайте, какие проверки были запущены и что не удалось проверить локально.

## 19. Если нужна нестандартная библиотека в Spark image

Обычные изменения PySpark-кода, SQL, config, README и migrations не требуют сборки нового образа: код доставляется в Spark pod через `git-sync`.

Custom Spark image нужен только когда runtime-зависимость должна быть внутри контейнера до старта job:

- новая Python-библиотека, которой нет в default Spark image;
- truststore, сертификат или системный бинарь;
- runtime-файл, который нельзя безопасно доставить через `git-sync`;
- зависимость нужна и driver, и executors.

Процесс для новой Python-библиотеки:

1. Добавить библиотеку в `requirements.txt` рядом с Dockerfile того образа, который действительно используется job.
2. Обновить Dockerfile так, чтобы он устанавливал зависимости из этого `requirements.txt`. Версии должны быть зафиксированы.
3. Если пакет внутренний, установка должна идти через Nexus с credentials из Drone secrets. Credentials нельзя коммитить в репозиторий.
4. Обновить SparkApplication image в config, если меняется имя или tag образа.
5. Сделать PR, дождаться проверок и влить изменения в `master`.
6. После merge в `master` один раз создать tag для сборки образа. Для ranking upload сейчас используется формат `spark-feature-platform-ranking-upload-*`.
7. Дождаться Drone build по tag: он соберет Docker image и опубликует его в registry.
8. После публикации образа проверить, что DAG использует нужный image tag и job стартует с новой зависимостью.

Важно: tag создается после merge, потому что Drone собирает образ из состояния `master` на момент tag. Повторно создавать tag нужно только при следующем изменении runtime-зависимостей или Dockerfile. Для обычных изменений job-кода новый tag не нужен.

## 20. Минимальный шаблон финального ответа по новой таблице

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
- базовые repository contracts и layer configs читаются корректно;
- dbt source sync видит новую таблицу и сможет создать DQ definitions;
- Iceberg maintenance sync добавляет только repository-managed таблицы;
- ranking upload config валиден, если он менялся;
- whitespace-проверка прошла.
```
