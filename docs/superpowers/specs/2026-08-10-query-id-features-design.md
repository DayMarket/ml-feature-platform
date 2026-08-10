# Уплотнение поисковых фичей по query_id

Дата: 2026-08-10. Команда: search. Статус: gold-таблицы реализованы, DAG на паузе.
План: `docs/superpowers/plans/2026-08-10-query-id-features.md`. Публикация в ranking upload
отложена и в этой итерации не делалась.

**Границы первой итерации:** реализуются только две gold-таблицы. Разделы про ranking upload
(новые feature groups, перенос аплоад-DAG на 07:00, пересчёт дельт, `ranking_service_input.yaml`)
описывают целевое состояние, но в первую итерацию не входят: `upload/features_service_upload/v1/**`
не редактируется. Пока публикации нет, аплоад остаётся в 04:00 и конфликта расписаний не возникает.

## Задача

`gold.feature_platform_search_query_id` даёт каноничный `query_id`: разные формулировки одного
запроса получают один идентификатор. Сейчас поисковые фичи считаются на уровне отдельного
`query_text`, поэтому редкий запрос получает разреженную и шумную статистику, хотя его
сформулированный иначе «близнец» может иметь достаточную историю.

Нужно посчитать те же фичи, агрегируя события **в рамках `query_id`**, но отдать результат
по-прежнему **на исходных `query_text`** — ключ джойна в ranking-сервисе (`search_query`) не
меняется.

## Что заливается в сервис сейчас

`upload/features_service_upload/v1/config.yaml` → Kafka `ranking.features.updates`, DAG
`feature_platform_ranking_features_upload_dag`. Шесть групп, все источники — `gold`:

| Feature group | Gold-источник | Entity keys | Фичей |
|---|---|---|---|
| `fs_search_query_skg_atc_order_features_v2` | `feature_platform_search_sku_group_id_query_atc_order_features_v2` | `query, sku_group_id` | 41 |
| `fs_search_skg_conversion_features_v2` | `feature_platform_sku_group_search_conversion_features_v2` | `sku_group_id` | 69 |
| `fs_search_skg_stock_features_v1` | `feature_platform_sku_group_stock_features` | `sku_group_id` | 6 |
| `fs_search_skg_price_features_v1` | `feature_platform_sku_group_price_features` | `sku_group_id` | 6 |
| `fs_search_query_atc_features_v1` | `feature_platform_search_query_atc_features` | `query` | 18 |
| `fs_search_skg_rating_v1` | `feature_platform_sku_group_feedback_base_stats` | `sku_group_id` | 1 |

Остальные входы модели (`dssm_score`, `normalized_linear_score`, `bid_amounts`, `alpha`…) имеют
schema `EXTERNAL`/`CONSTANT` и приходят не из этого репозитория.

`query_text` фигурирует в шести gold-таблицах, но в сервис уходят только две — обе перечислены
выше. Вне аплоада остаются `feature_platform_search_sku_group_id_query_atc_order_features` (v1),
`feature_platform_query_skg_aggregated_conversions_legacy`,
`feature_platform_query_skg_pairwise_features_legacy`,
`feature_platform_search_sku_group_id_query_atc_features`. **Они в скоуп не входят.**

## Скоуп

Пересобираются на `query_id` только две таблицы, которые реально доезжают до сервиса:

- `gold.feature_platform_search_query_atc_features` — грейн `date,query`
- `gold.feature_platform_search_sku_group_id_query_atc_order_features_v2` — грейн `date,query,sku_group_id`

Существующие таблицы и их группы аплоада **не меняются** — новые версии живут рядом, чтобы модель
могла сравнить query_text- и query_id-версии на A/B.

## Новые сущности

| Новая таблица | Путь | PK | Зеркалит | Колонок-фичей / в аплоаде |
|---|---|---|---|---|
| `iceberg.gold.feature_platform_search_query_atc_features_qid` | `layers/gold/query/search_query_atc_features_qid/v1` | `date,query` | `feature_platform_search_query_atc_features` | 24 / 18 |
| `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features_qid` | `layers/gold/query_sku_group_id/sku_group_query_atc_order_features_qid/v1` | `date,query,sku_group_id` | `..._query_atc_order_features_v2` | 61 / 41 |

DAG-идентификаторы:

- `feature-platform.layers.gold.query.search_query_atc_features_qid`
- `feature-platform.layers.gold.query_sku_group_id.sku_group_query_atc_order_features_qid`

Группы ranking upload:

- `fs_search_query_atc_features_qid_v1` — entity key `query`, те же 18 фичей, что в
  `fs_search_query_atc_features_v1`, в том же порядке
- `fs_search_query_skg_atc_order_features_qid_v1` — entity keys `query,sku_group_id`, те же 41 фича,
  что в `fs_search_query_skg_atc_order_features_v2`, в том же порядке

Обе группы добавляются в существующую модель `search_ranking_main`. Порядок фичей внутри группы
копируется из оригинала дословно: имена в сервис не передаются, передаётся только упорядоченный
вектор значений.

## Состав фичей

Строгое зеркало. Имена, формулы, окна (1, 3, 7, 14, 21, 30, 60, 90), `SMOOTHING_COEF = 100.0`,
границы окон и фильтры повторяются из оригинальных джобов дословно. Отличается только ключ
агрегации.

Дополнительно в каждой таблице две служебные колонки, которые **не входят в вектор аплоада**:

- `query_id STRING` — ключ группы, по которому фактически посчитаны значения строки, то есть
  `group_key`. Для строк с фолбэком он равен нормализованному `query_text`, а не `NULL`
- `has_query_id BOOLEAN` — `true`, если `query_text` нашёлся в справочнике, `false` для фолбэка.
  Именно эта колонка отличает настоящую группу от группы из одного элемента

Они нужны для отладки, замера покрытия и DQ. `scripts/validate_ranking_upload_configs.py` требует,
чтобы все фичи группы присутствовали в миграциях, но не запрещает лишние колонки в таблице.

## Алгоритм

### Нормализация

Единый трансформ, тот же, что уже используют оба оригинальных джоба:

```
lower(x) -> replace("ё", "е") -> replace(r"\s+", " ") -> trim
```

Применяется к:

- `uniqs` из `silver.feature_platform_search_sku_group_id_install_query`
- `query` из `silver.feature_platform_sku_group_query_search_orders`
- `query_text` из `gold.feature_platform_search_query_id`

Нормализация `query_text` обязательна: справочник хранит **сырой** текст запроса, как он пришёл в
`uniqs`, без приведения регистра. Без общей нормализации джойн со справочником даёт систематические
промахи.

`query_id` дополнительно проходит `lower` + `trim` (анализатор ES уже отдаёт нижний регистр, шаг
защитный).

Строки с пустым результатом нормализации отбрасываются — как в текущих джобах.

### Карта групп

```python
qid = (
    spark.table("iceberg.gold.feature_platform_search_query_id")
    .filter(F.col("version") == F.lit("v1"))
    .select(
        normalize(F.col("query_text")).alias("query"),
        F.trim(F.lower(F.col("query_id"))).alias("query_id"),
    )
    .filter(F.col("query").rlike(r"\S") & F.col("query_id").rlike(r"\S"))
    .groupBy("query")
    .agg(F.min("query_id").alias("query_id"))
)
```

`groupBy` + `min` нужен потому, что справочник имеет PK `query_text,version` на **сыром** тексте:
несколько сырых вариантов могут схлопнуться в один нормализованный `query`. `min` даёт
детерминированный результат при перезапуске; при корректной работе справочника такие варианты
всё равно получают один и тот же `query_id`, и выбор не влияет на значения.

Фильтр `version = 'v1'` захардкожен как константа `QUERY_ID_VERSION` в обоих job-файлах, а не
вынесен в `config.yaml`. Переключение на будущую `v2` означает правку этой константы в обоих
джобах; если её всё же понадобится сделать конфигурационной, для этого потребуется новый плейсхолдер
в общем Spark-шаблоне (`config/spark/layer_spark_application.yaml` сейчас пробрасывает только
`--partition_start`, `--partition_end` и `--table_name`).

### Ключ группировки

```python
group_key = F.coalesce(F.col("query_id"), F.col("query"))
has_query_id = F.col("query_id").isNotNull()
```

Запрос, которого нет в справочнике, образует группу из самого себя. Строка всё равно попадает в
финальную таблицу, её фичи считаются ровно так же, как считаются сейчас. Покрытие итоговой
таблицы — 100 % от текущего универсума, деградация плавная.

### Расчёт и разворот

1. События и заказы нормализуются, к ним `LEFT JOIN` карты групп, считается `group_key`.
2. Все оконные суммы агрегируются по `group_key` (таблица уровня запроса) или по
   `group_key, sku_group_id` (парная таблица) вместо `query` / `query, sku_group_id`.
3. Знаменатели уровня sku (`skg_smooth_atcs_*`, `skg_smooth_orders_*`) остаются сгруппированными
   только по `sku_group_id` — они не зависят от запроса и не меняются.
4. Фильтры парной таблицы (`query_skg_uniq_impressions_14 >= 2` и
   `query_skg_uniq_atcs_90 > 0 OR query_skg_uniq_orders_90 > 0`) применяются **на уровне группы,
   до разворота**.
5. Разворот: `members(query -> group_key, has_query_id) INNER JOIN group_features ON group_key`.

**Полное уплотнение.** В парной таблице каждый `query_text` группы получает все пары
`(group_key, sku_group_id)`, прошедшие фильтры, включая пары с sku, которые с этим конкретным
`query_text` никогда не встречались. Это и есть цель задачи.

Универсум `query_text` в обеих таблицах совпадает с универсумом оригинальных таблиц: все
нормализованные запросы с показами в окне `[ds - 90, ds - 1]`. Новых `query_text` не появляется —
появляются новые пары `(query_text, sku_group_id)`.

## Оркестрация

Расписания UTC. Все DAG используют `CronDataIntervalTimetable`, поэтому logical date прогона равна
началу интервала, то есть предыдущим суткам.

```
05:00  feature-platform.layers.gold.query_text_version.search_query_id        (без изменений)
06:00  feature-platform.layers.gold.query.search_query_atc_features_qid       (новый)
06:00  feature-platform.layers.gold.query_sku_group_id.
       sku_group_query_atc_order_features_qid                                 (новый)
07:00  feature_platform_ranking_features_upload_dag                           (перенос с 04:00)
```

Сенсоры новых DAG (`ExternalTaskSensor`, `mode="poke"`, `poke_interval=30`, `timeout=6h`,
`check_existence=True`):

| Внешний DAG | `execution_delta` |
|---|---|
| `feature-platform.layers.gold.query_text_version.search_query_id` | 1 час |
| `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq` | 5 часов |
| `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_query_search_orders.dq` | 5 часов |

Зависимость от `query_id` вешается на **сам DAG** `search_query_id`, а не на его DQ-прогон.
Проверка: logical date нового DAG — `D-1 06:00`, минус 1 час даёт `D-1 05:00`, что равно logical
date прогона `search_query_id`, физически стартующего в `D 05:00`, за час до нашего `D 06:00`.

### Перенос аплоада

Аплоад-DAG стоит в 04:00 и не может дождаться таблицы, которая появится в 06:00:
`scripts/validate_ranking_upload_configs.py` требует
`dependency_execution_delta_minutes >= 0`, а отрицательная дельта и по смыслу означала бы
ожидание будущего. Поэтому аплоад переносится на 07:00, а все существующие дельты сдвигаются
на `+180` минут.

| Feature group | Расписание источника | Дельта сейчас | Дельта после переноса |
|---|---|---|---|
| `fs_search_query_skg_atc_order_features_v2` | `0 3 * * *` | 60 | 240 |
| `fs_search_skg_conversion_features_v2` | `0 3 * * *` | 60 | 240 |
| `fs_search_skg_stock_features_v1` | `0 3 * * *` | 60 | 240 |
| `fs_search_skg_price_features_v1` | `0 2 * * *` | 120 | 300 |
| `fs_search_query_atc_features_v1` | `0 3 * * *` | 60 | 240 |
| `fs_search_skg_rating_v1` | `10 3 * * *` | 50 | 230 |
| `fs_search_query_atc_features_qid_v1` | `0 6 * * *` | — | 60 |
| `fs_search_query_skg_atc_order_features_qid_v1` | `0 6 * * *` | — | 60 |

Операционное следствие: фичи в ranking-сервисе начнут обновляться на три часа позже. Это
осознанное решение в пользу однодневной цепочки `query_id → фичи → аплоад`.

## Рантайм

Стандартный Spark-паттерн репозитория, без нового образа: shared
`config/spark/layer_spark_application.yaml`, доставка кода через `git-sync`, профиль ресурсов из
`config/spark/resources.yaml`.

- таблица уровня запроса — профиль `small`, как у оригинала
- парная таблица — профиль `large`, как у оригинала

Запись — `writeTo(target).overwritePartitions()`, партиционирование по `date`.

Разбор границы интервала делается через тестируемый хелпер, принимающий
`2026-06-17T00:00:00`, `2026-06-17T00:00:00+00:00`, `2026-06-17T00:00:00Z`,
`2026-06-17 00:00:00+00:00` и `YYYY-MM-DD HH:MM:SS`. Срез `partition_start[:10]` из v2 не
переносится — AGENTS.md это прямо запрещает.

## DQ

Отдельные табличные DQ-тесты не добавляются: сгенерированного dbt-source DQ достаточно, а
проверки на строгий рост числа строк были бы шумными на старте. `scripts/sync_dbt_sources.py`
подхватывает новые `config.yaml` автоматически; после мержа в master нужно проверить
сгенерированные PR в dbt-trino и в `DayMarket/pyspark-etl` (Iceberg maintenance).

## Тесты

Новый файл `ci_test/test_query_id_features.py`:

- нормализация: регистр, `ё`, повторяющиеся пробелы, обрезка краёв, отбрасывание пустых
- карта групп: детерминированность `min(query_id)` при схлопывании сырых вариантов, фильтр по
  `version`
- `group_key`: подстановка `query_text` при отсутствии в справочнике, корректный `has_query_id`
- разворот: все исходные `query_text` присутствуют в выходе; `query_text` из одной группы получают
  одинаковые значения фичей
- полное уплотнение: `query_text` получает пару с `sku_group_id`, который встречался только у
  другого члена группы
- хелпер разбора даты: все перечисленные форматы плюс понятная ошибка на неподдерживаемом значении

Существующие тесты, требующие обновления: `ci_test/test_validate_ranking_upload_configs.py` (две
новые группы), `ci_test/test_sync_dbt_sources.py` и `ci_test/test_sync_iceberg_maintenance.py`
(две новые таблицы) — при условии, что они проверяют полный список.

## Замеры

Все числа сняты 2026-08-10 по Trino на дате партиции `2026-08-09`.

### 1. Покрытие справочника — блокер

Состояние справочника на 2026-08-10: 2 839 647 `query_text` → 2 169 744 `query_id`, средний
размер группы **1.30**. Распределение записей по `updated_at`:

```
2026-08-07 05:00   884 330
2026-08-08 05:00   690 049
2026-08-09 05:00   634 051
2026-08-10 05:00   631 217
```

Это четыре обычных ежедневных прогона. **Бэкфилла в таблице нет**: справочник содержит запросы,
впервые увиденные за 4 дня, а не за 90, при том что окна фичей доходят до 90 дней.

Покрытие в трёх срезах, дата партиции `2026-08-09`:

| Срез | Покрытие |
|---|---|
| Строки парной таблицы `query × sku_group_id` | 90 % |
| Показы `query_uniq_impressions_30` в таблице уровня запроса | 81.5 % |
| Различные `query_text` за 90 дней | **7.5 %** (2 370 640 из 31 413 234) |

Справочник покрывает голову трафика и почти не покрывает хвост. Это бьёт по цели задачи:
уплотнение нужно в первую очередь редкому запросу без собственной истории, а покрыты сейчас
именно те запросы, у которых история и так достаточная. На покрытых 81.5 % трафика выигрыш от
уплотнения будет минимальным.

**Вывод: бэкфилл `search_query_id` по историческим 90 дням — предусловие задачи, а не
опциональное улучшение.** До него реализовывать имеет смысл только ради готовности пайплайна;
эффекта на метрики ждать не стоит.

```sql
-- размер справочника и средний размер группы
SELECT
    version,
    count(*)                                        AS rows_cnt,
    count(DISTINCT query_id)                        AS uniq_query_id,
    count(*) * 1.0 / count(DISTINCT query_id)       AS avg_group_size
FROM "dwh-iceberg".gold.feature_platform_search_query_id
GROUP BY version;

-- доля query_text из 90-дневного окна, покрытых справочником
WITH src AS (
    SELECT DISTINCT
        trim(regexp_replace(replace(lower(uniqs), 'ё', 'е'), '\s+', ' ')) AS query
    FROM "dwh-iceberg".silver.feature_platform_search_sku_group_id_install_query
    WHERE space = 'SEARCH_RESULTS'
      AND date >= current_date - interval '90' day
      AND date <  current_date
),
dict AS (
    SELECT DISTINCT
        trim(regexp_replace(replace(lower(query_text), 'ё', 'е'), '\s+', ' ')) AS query
    FROM "dwh-iceberg".gold.feature_platform_search_query_id
    WHERE version = 'v1'
)
SELECT
    count(*)                                              AS queries_total,
    count(dict.query)                                     AS queries_covered,
    count(dict.query) * 1.0 / count(*)                    AS coverage
FROM src LEFT JOIN dict ON src.query = dict.query;
```

Если покрытие низкое — до включения аплоада нужен бэкфилл `search_query_id` по историческим 90
дням. Это отдельная задача, в текущий скоуп не входит.

### 2. Рост числа строк

**Таблица уровня запроса не растёт.** Универсум `query_text` тот же, меняются только значения:
31 413 234 строки на дату как было, но посчитанных на меньшем числе групп.

**Парная таблица растёт.** Замер на дате `2026-08-09`, разворот по парам, уже присутствующим в
текущей таблице:

| Метрика | Значение |
|---|---|
| Строк сейчас | 5 957 253 |
| Уникальных `query` | 707 336 |
| Групп после схлопывания | 551 011 (сжатие 1.28×) |
| Строк после полного уплотнения | 19 898 755 |
| **Рост** | **3.34×** |

Распределение неравномерное: p99 запросов в группе — 5, максимум — 121; максимум
`sku_group_id` в группе — 5 707; самая большая группа даёт 336 713 строк. Топ-10 групп дают
6.5 % выхода, топ-100 — 20 %, топ-1000 — 44 %. То есть 0.18 % групп производят почти половину
строк.

Две поправки к числу 3.34×, обе вверх:

1. Замер сделан по парам, уже присутствующим в текущей таблице. На уровне группы фильтры
   (`uniq_impressions_14 >= 2`, `atcs_90 > 0 OR orders_90 > 0`) пропустят и те пары, что
   поодиночке не проходили.
2. Главное: 3.34× получены при среднем размере группы **1.30**, то есть на неполном справочнике.
   После бэкфилла (см. риск 1) хвост попадёт в группы, и средний размер уйдёт в район
   `31.4M / 2.2M ≈ 14`. Рост масштабируется примерно линейно по размеру группы.

| Сценарий | Средний размер группы | Строк/день, парная | Рост |
|---|---|---|---|
| Текущий справочник (4 дня) | 1.30 | ~20M | 3.3× (замер) |
| После бэкфилла на 90 дней | ~14 | ~200M | ~35× (оценка порядка) |

Вторая строка — прикидка по порядку величины, не замер: точное число зависит от распределения
хвостовых запросов по группам, и его надо переснять сразу после бэкфилла.

**Принятое решение: отсечки нет, пишутся все пары.** Вариант с топ-N `sku_group_id` внутри группы
рассматривался и отклонён — таблицы и аплоад строятся на полном разворачивании. Рост объёма принят
как осознанная цена уплотнения.

Операционное следствие, которое надо держать в виду при выкатке: после бэкфилла парная таблица
выходит на порядок ~200M строк в сутки, и весь этот объём идёт в Kafka и в ranking-сервис. Перед
включением групп аплоада стоит переснять фактический размер и согласовать его с владельцами
топика и сервиса. Это вопрос ёмкости, а не изменения дизайна.

## Что остаётся за скоупом

- **бэкфилл справочника `search_query_id` по историческим 90 дням** — отдельная задача, но она
  является предусловием для получения эффекта от этой (см. риск 1). Реализация пайплайна может
  идти параллельно с бэкфиллом, включение групп аплоада — только после него и после повторного
  замера роста строк
- уплотнение legacy- и v1-таблиц на `query_text`
- любые новые фичи, не являющиеся зеркалом текущих (density-фичи уровня группы, размер группы,
  доля запроса внутри группы) — обсуждается отдельно после первых замеров
- вывод из эксплуатации старых query_text-групп аплоада
