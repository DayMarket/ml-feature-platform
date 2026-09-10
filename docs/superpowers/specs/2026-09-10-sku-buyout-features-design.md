# Витрина `feature_platform_sku_buyout_features` и выгрузка в PostgreSQL

Дата: 2026-09-10. Статус: дизайн согласован, реализация не начата.

## Задача

Сервису невыкупов нужна одна строка на `sku_id` с экономикой корзины (1p/3p,
комиссия, себестоимость), выкупаемостью на всех уровнях и категорийным деревом.
Потребитель читает таблицу `sku_buyout_features` в PostgreSQL `mlgrowth`.

Репозиторий требует, чтобы gold-выход был Iceberg-таблицей (`AGENTS.md`,
«Repository Purpose»), поэтому работа делится на два артефакта: витрину в Iceberg
и процесс публикации в PostgreSQL.

## Артефакт 1: gold-энтити

- Путь: `layers/gold/sku_id/sku_buyout_features/v1`
- Таблица: `iceberg.gold.feature_platform_sku_buyout_features`
- Primary key: `date,sku_id`, партиционирование по `date`
- DAG id: `feature-platform.layers.gold.sku_id.sku_buyout_features`
- `dag.group_tag`: `buyout-features`
- Расписание: `0 7 * * *` UTC
- Владелец: `table.meta.team = team:buyer`, `dag.team = buyer`,
  `dag.owner = team:buyer`, `alerts.team = buyer`, severity `P2`,
  `oncall_webhook_conn_id = team:buyer`
- Рантайм: Trino-source Airflow/Python + `pyiceberg`, образ
  `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, connection `trino_bx_analytics`
- Дата партиции: `previous_utc_date(data_interval_end)` — дословно то же правило,
  что у `buyout_online_sku_features`, иначе будет прочитана чужая партиция

### Зависимость

Сенсор `ExternalTaskSensor` на таску `dq` DAG'а
`feature-platform.layers.gold.sku_id.buyout_online_sku_features`
(06:00 UTC), `execution_delta = 1h`, `mode="reschedule"`.

### Шардирование

`source.shards: 8`, срез задаётся **диапазоном** `sku_id`, не остатком от деления.

Причина зафиксирована замером плана Trino. Оба предиката пушдаунятся в
Postgres-коннектор, но по-разному:

```
WHERE id >= 1000000 AND id < 2000000
  TableScan[table = kazanexpress:public.sku ... constraint on [id] ...]
  Estimates: {rows: 1310347}

WHERE id % 8 = 3
  TableScan[table = kazanexpress:public.sku ...
            constraints=[ParameterizedExpression[expression=(("id") % (?)) = (?)]]]
  Estimates: {rows: 1}
```

Диапазон по `id` (это `bigserial`, первичный ключ) — index range scan, поэтому
8 срезов суммарно читают OLTP один раз. Остаток от деления индексом
воспользоваться не может: каждый срез вызовет полный seq scan, то есть 8 полных
проходов по живой OLTP-базе ежедневно. Оценка `rows: 1` во втором плане
недостоверна и маскирует эту стоимость.

Границы срезов считаются в рантайме из `min(id)`/`max(id)` по
`"dwh-iceberg".silver.sku` (дёшево, Iceberg). У первого среза нет нижней границы,
у последнего — верхней, чтобы sku, заведённые между расчётом границ и чтением,
не потерялись.

Запись — по образцу `buyout_online_account_features`:
`write_partition_shard(..., replace=(shard == 0))`. Первый срез перезаписывает
партицию, остальные дописываются; повтор таски начинается с первого среза,
поэтому запись идемпотентна.

### Источники

| Таблица | Роль |
|---|---|
| `"dwh-iceberg".silver.sku` | база строк (10 485 803 sku), `product_id`, `seller_id`, `category_id`, габариты |
| `kazanexpress.public.sku` | `commission` для 3p |
| `"dwh-clickhouse".marts.stock_flow_1p` | себестоимость 1p (последняя приёмка) |
| `"dwh-clickhouse".dict.seller` | `is_1p` |
| `"dwh-clickhouse".dict.category` | дерево `l1..l5_category` |
| `"dwh-iceberg".gold.feature_platform_buyout_online_sku_features` | выкупаемость, репозиторно-управляемая |

### Разрешение 1p/3p

Проверено в Trino: `kazanexpress.public.sku` содержит все 10 486 376 sku, `id`
уникален, `commission` никогда не NULL. Ветка 3p из исходного запроса не
фильтрует продавца, поэтому возвращает в том числе все 179 081 sku ветки 1p.
`UNION ALL` задвоил бы их; `UNION` без `ALL` их не схлопнул бы — строки
различаются колонкой `type`.

Решение — антиджойн, а не union:

```sql
one_p AS (   -- 179 081 строк
    SELECT sku_id, value_per_piece
    FROM (последняя ACCEPTANCE по sku_id из stock_flow_1p)
    WHERE value_per_piece > 0
      AND продавец имеет is_1p = 1
)
...
CASE WHEN one_p.sku_id IS NOT NULL THEN '1p' ELSE '3p' END AS type,
CASE WHEN one_p.sku_id IS NULL THEN ke.commission END      AS commission,
one_p.value_per_piece                                      AS cost_price
```

Один ряд на `sku_id` гарантирован построением, а не совпадением данных.

Согласованные следствия:

- 34 979 sku принадлежат 1p-продавцам, но себестоимости не имеют
  (214 060 − 179 081). Они получают `type = '3p'` и комиссию. Это осознанное
  решение владельца: 1p-товар помечается как 3p, зато в витрине нет строк без
  экономики.
- 25 256 sku имеют приёмку на 1p-складе при 3p-продавце. Фильтр `is_1p = 1` их
  отсекает: «приёмка на 1p-складе» и «1p-товар» — разные вещи.

### Категорийное дерево

Джойн `silver.sku.category_id = dict.category.id` покрывает 100% строк
(10 485 803 из 10 485 803), справочник — 6 913 категорий.

Каскад «прокинуть вниз последний ненулевой уровень» продлён до `l2`
относительно исходного запроса. Замер нулей по справочнику: `l1` — 0,
`l2` — 23, `l3` — 255, `l4` — 2 077, `l5` — 5 645. Исходный каскад начинался с
`l3`, поэтому у 23 категорий с `l2_category = 0` он прокинул бы вниз ноль
вместо `l1`. `l1_category` не бывает нулевым, так что каскад на нём кончается.

### Схема

Признаки выкупаемости берутся в `_shrunk`-версиях везде, где они существуют.
`COALESCE`-каскад из исходного запроса не нужен: `_shrunk`-колонки уже содержат
подстановку родителя (см. README `buyout_online_sku_features`).

| Колонка | Тип | Источник |
|---|---|---|
| `date` | `DATE` | партиция |
| `sku_id` | `BIGINT` | `silver.sku.id` |
| `product_id` | `BIGINT` | `silver.sku.product_id` |
| `seller_id` | `BIGINT` | `silver.sku.seller_id` |
| `category_id` | `BIGINT` | `silver.sku.category_id` |
| `l1_category`…`l5_category` | `BIGINT` | `dict.category`, каскад выше |
| `type` | `VARCHAR` | `'1p'` / `'3p'` |
| `commission` | `DECIMAL(5,2)` | `kazanexpress.public.sku.commission`, NULL у 1p |
| `cost_price` | `BIGINT` | `stock_flow_1p.value_per_piece`, NULL у 3p |
| `is_not_block` | `BOOLEAN` | константа `false` |
| `sku_buyout` | `DOUBLE` | `sku_buyout_rate_shrunk_90d` |
| `product_buyout` | `DOUBLE` | `product_buyout_rate_shrunk_90d` |
| `shop_buyout` | `DOUBLE` | `shop_buyout_rate_shrunk_90d` |
| `category_buyout` | `DOUBLE` | `category_buyout_rate_90d` |
| `category_no_show` | `DOUBLE` | `category_no_show_rate_90d` |
| `sku_n_delivered` | `BIGINT` | `sku_n_delivered_90d` |
| `product_n_delivered` | `BIGINT` | `product_n_delivered_90d` |
| `predicted_dimensional_group` | `VARCHAR` | габариты `silver.sku` |

Решения по схеме, отличающиеся от присланного DDL:

- `category_buyout` и `category_no_show` не имеют `_shrunk`-варианта: они уже
  сглажены к общей выкупаемости маркетплейса в витрине-источнике.
- `seller_id` в `feature_platform_buyout_online_sku_features` отсутствует (там
  есть `shop_id`), поэтому берётся из `silver.sku`.
- `sku_n_delivered` и `product_n_delivered` — `BIGINT`, а не `integer`:
  источник отдаёт `BIGINT`.
- `updated_at` в gold не хранится: версией партиции служит `date`. Значение
  проставляется на стороне выгрузки в PostgreSQL.
- `is_not_block` заводится колонкой со значением `false`. Источника правила не
  существует ни в одном из присланных запросов, ни в одной из 29 таблиц
  `mlgrowth`. Колонка занимает место в контракте, чтобы её наполнение не стало
  изменением схемы.
- `predicted_dimensional_group` сохраняется: он есть и в исходном запросе, и в
  живой `mlgrowth.public.sku_commission_info`, хотя в присланном DDL отсутствует.

### Скоуп строк

База — весь sku-универс `silver.sku` (10 485 803), а не выдача витрины
выкупаемости (2 889 958). Следствие: у ~7.6 млн sku все шесть buyout-колонок
будут NULL. Это согласованное решение; оно приближает витрину к текущей
`mlgrowth.public.sku_commission_info` (10 690 898 строк).

Витрина выкупаемости покрывает 2 889 957 из 2 889 958 своих sku в `silver.sku`
— один sku_id в gold не имеет строки в silver. Джойн LEFT, так что это не
уронит запись.

## Артефакт 2: выгрузка в PostgreSQL

- Путь: `upload/buyout_sku_postgres_upload/v1`
- DAG id: `feature-platform.upload.buyout_sku_postgres_upload`
- Расписание: `0 7 * * *` UTC, сенсор на таску `dq` витрины, `execution_delta = 0`
- Соединение: `postgres_non_buyout_service_connect`, schema `mlgrowth`
- Цель: `mlgrowth.sku_buyout_features`
- Режим записи: `TRUNCATE` + `COPY` в одной транзакции. Читатели видят либо
  старое содержимое целиком, либо новое целиком.
- `updated_at` проставляется временем прогона выгрузки

Это первый в репозитории процесс публикации не в Kafka.

### Контракт целевой таблицы

`mlgrowth.sku_buyout_features` уже создана владельцем сервиса. 21 колонка:

```
sku_id bigint, product_id bigint, seller_id bigint,
l1_category integer, l2_category integer, l3_category integer,
l4_category integer, l5_category integer,
type varchar, commission numeric, cost_price numeric,
predicted_dimensional_group varchar, is_not_block boolean,
sku_buyout double, product_buyout double, category_buyout double,
shop_buyout double, category_no_show double,
sku_n_delivered integer, product_n_delivered integer,
updated_at timestamptz
```

Расхождения с витриной, которые обязана закрыть выгрузка:

- **`category_id` в целевой таблице нет.** В gold он остаётся: это ключ джойна с
  `dict.category` и линия происхождения `l1..l5_category`. Выгрузка проецирует
  ровно 21 перечисленную колонку и `category_id` не отправляет.
- **`date` в целевой таблице нет.** Версию среза несёт `updated_at`; выгрузка
  всегда публикует последнюю партицию витрины целиком.
- **`sku_n_delivered` и `product_n_delivered` — `integer`**, тогда как в витрине
  и в источнике это `BIGINT`. Замер на партиции 2026-09-09: максимум обоих —
  234 462, то есть запас до предела `int4` четырёхзначный. Выгрузка приводит тип
  явно; при переполнении она должна падать, а не молча обрезать.
- **`l1..l5_category` — `integer`**, тогда как `dict.category` отдаёт
  `decimal(20,0)`. В gold хранятся как `BIGINT`, приведение — на выгрузке.
- **`commission` и `cost_price`** Trino показывает как `varchar`. Это его
  штатное отображение postgres-типа `numeric` без явных precision/scale, а не
  текстовая колонка. Для `COPY` разницы нет: значения пишутся десятичными
  литералами, PostgreSQL приводит их сам. Замеренные максимумы —
  `commission` = 40.00, `cost_price` = 58 934 464.

### Что придётся починить в CI

`scripts/validate_ranking_upload_configs.py:394` глобит `upload/**/config.yaml`
без разбора и требует непустой `feature_groups`, поэтому Postgres-конфиг уронит
шаг валидации.

План:

- добавить в конфиг выгрузки дискриминатор `sink.type` (`kafka` / `postgres`);
- в валидаторе пропускать ranking-специфичные проверки (`models`, уникальность
  имён feature group) при `sink.type != "kafka"`, сохранив проверку колонок
  против миграций источника — она полезна для любой выгрузки;
- добавить регрессионный тест в `ci_test/`.

Форма `feature_groups[].source.dependency_dag_id` сохраняется, поэтому
`scripts/generate_feature_platform_map.py:691` нарисует ребро карты без правок.

## DQ и feature_stats

Штатные шаги DAG'а витрины, оба терминальные и параллельные друг другу:
`<materialize> >> [dq_task, stats_task]`.

`dq`: базовый набор. `primary_key_not_null` и `primary_key_unique` — в `error`
(это витрина признаков). Объёмные тесты и `freshness` — в `warn` на время
раскатки, как у соседних buyout-энтити; порог `row_count_min` ставится по
наблюдённому объёму 10.49 млн строк и сопровождается комментарием в
`config.yaml`.

`feature_stats`: ежедневный скан партиции в 10.5 млн строк в общем Trino.
`exclude_columns`: `product_id`, `seller_id`, `category_id`,
`l1_category`…`l5_category` — это идентификаторы, а не признаки.

## Известные риски и открытые вопросы

- **Конфликт производителей.** `mlgrowth.public.sku_commission_info` содержит
  10 690 898 строк с `updated_at` = 2026-09-10 11:20 и является подмножеством
  новой витрины по смыслу. Кто её пишет, из этого репозитория не видно. До
  включения выгрузки нужно решить, гасится ли тот процесс.
- **Комиссия читается из живой OLTP.** Диапазонное шардирование сводит нагрузку
  к одному индексному проходу, но зависимость от `kazanexpress.public.sku`
  остаётся. Зеркало `"dwh-iceberg".silver.sku_commission_tracking` совпадает с
  OLTP на 99.90% (10 475 823 из 10 486 546; 5 012 расходятся, 5 711 не покрыты)
  и может стать заменой, если нагрузка на OLTP окажется неприемлемой.
- **Внешний эффект мержа в `master`.** При дефолтных `create_dbt_pr` и
  `create_maintenance_pr` master-side CI заведёт PR в `DayMarket/dbt-trino` и в
  `DayMarket/pyspark-etl` Iceberg maintenance.
- **Схема целевой таблицы принадлежит владельцу сервиса, а не этому
  репозиторию.** `mlgrowth.sku_buyout_features` создана вне репозитория, её DDL
  здесь не версионируется. Добавление или удаление колонки на той стороне ломает
  `COPY` молча — список колонок в конфиге выгрузки нужно держать синхронным
  вручную и сверять при каждом изменении витрины.

## Источник фактов

Все числа получены запросами к Trino 2026-09-10 через MCP. Из репозитория взяты
только контракты: `AGENTS.md`, `layers/gold/sku_id/buyout_online_sku_features/v1`,
`layers/gold/account_id/buyout_online_account_features/v1`,
`upload/buyout_features_upload/v1`, `scripts/validate_ranking_upload_configs.py`,
`scripts/generate_feature_platform_map.py`.
