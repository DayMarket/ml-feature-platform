# Дизайн: датасет `ranking_logs` v1

Дата: 2026-09-01
Статус: согласован с заказчиком, готов к написанию плана реализации

## 1. Задача

Собрать датасет для подбора параметров ранжирующей формулы поиска. Источник —
`"dwh-iceberg".silver.ranking_analytics_events`: лог запросов к ранжирующему сервису,
где на каждый запрос записаны кандидаты и все составляющие финального скора.

Датасет разворачивает лог до уровня «запрос × кандидат», добавляет разложение формулы
(alpha/beta/gamma/delta-составляющие), её входы (`cm2_features`) и внешние скоры
(`dssm_score`, `linear_score`, `normalized_linear_score`, `cpo_adv_percents`, `bid_amounts`),
после чего обогащает данные возрастом sku-группы, рейтингом и частотностью запроса.

Датасет предназначен только для офлайн-подбора параметров и анализа. Он не выгружается
в ranking-service и никакой другой онлайн-контур.

## 2. Контракт

| | |
|---|---|
| Путь | `datasets/search/ranking_logs/v1/` |
| Таблица | `iceberg.silver.feature_platform_ranking_logs_dataset_v1` |
| DAG id | `feature-platform.datasets.search.ranking_logs.v1` |
| Расписание | `0 12 * * 0` UTC (воскресенье, 12:00) |
| Партиция | `collection_date` |
| Первичный ключ | `collection_date, event_date, request_id, sku_group_id` |
| Владелец | `table.meta.team: team:search`, `dag.team: search`, `alerts.team: search` |
| Алерты | severity `P4`, `oncall_webhook_conn_id: oncall_webhook_search` |
| Движок | Spark (SparkApplication), профиль `search_dataset` |

### Окно данных

`data_interval` недельный, поэтому один ран покрывает ровно 7 календарных суток:

```
event_date ∈ [ date(data_interval_start), date(data_interval_end) - 1 day ]
collection_date = date(data_interval_end)
```

При запуске в воскресенье в 12:00 UTC это даёт воскресенье…субботу включительно,
то есть 7 закрытых суток, последняя из которых — вчерашняя. В таблице всегда две даты:
`collection_date` (одна на ран, она же партиция) и `event_date` (ровно 7 значений).

`collection_date` берётся от `data_interval_end`, а не от `data_interval_start`, чтобы
совпадать с датой фактического запуска DAG'а. Это отличается от `datasets/search/search_ranking/v1`,
где партиция считается от `data_interval_start`; отступление осознанное. Шаблоны `dq` и
`feature_stats` обязаны дословно совпадать между собой и использовать тот же `data_interval_end`:

```
partition_column: collection_date
partition_date_template: '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d") }}'
```

## 3. Отбор данных

### Фильтр по модели

Одна модель на ран, имя задаётся в `config.yaml` одной строкой. Текущее значение —
`search_unified_model_v9_cold_start`. В источнике за 2026-08-25 присутствовало 11 моделей,
целевая дала 481 982 запроса за сутки.

### Сэмплирование

Детерминированный отбор по запросу, а не по строке-кандидату: если запрос попал в выборку,
берутся все его кандидаты без обрезки по позиции.

```
abs(xxhash64(to_utf8(request_id))) % 10000 < sample_percent * 100
```

`sample_percent` — параметр `config.yaml`, дефолт **7**. Хэш от `request_id` даёт
воспроизводимость: перезапуск за ту же неделю вернёт ту же выборку, и не требуется shuffle.

Стратификации по `frequency_group` нет сознательно. Случайный отбор по запросам сохраняет
долю HF/MF/LF ровно такой, какая она в реальном трафике, а подбор параметров оптимизирует
формулу именно под реальный трафик. Стратификация внесла бы смещение, которое пришлось бы
компенсировать весами на анализе. `frequency_group` остаётся колонкой, поэтому стратифицировать
выборку постфактум можно.

### Ожидаемый объём

Замеры на источнике: 1098 запросов за минуту дали 919 022 кандидата, то есть ~837 кандидатов
на запрос (встречаются запросы с 2200 кандидатами).

| | строк |
|---|---|
| полный разворот | ~403 млн/день |
| `sample_percent = 7` | ~28 млн/день, **~200 млн за ран** |

Строка узкая (~35 скалярных колонок), 145-мерный `model_input['input']` в датасет не пишется.
Профиль `search_dataset` берётся как стартовый; объём на порядок больше существующего
`search_ranking/v1`, поэтому после первого рана метрики Spark пересматриваются и, вероятно,
заводится отдельный профиль в `config/spark/resources.yaml`.

## 4. Схема

Все массивы источника выровнены 1:1 с `ranking_candidates` — проверено на 7424 событиях:
`model_output`, `model_input['input']`, `model_input['cm2_features']`, `final_scores` и
`external_features.linear_score` имеют ту же длину во всех строках. Разворот идёт по общему
индексу `position`.

`ranking_candidates` содержит `sku_group_id`: из 130 275 уникальных кандидатов 130 274 нашлись
в `silver.sku.sku_group_id` и только 107 105 — в `silver.sku.id`.

| Колонка | Тип | Источник |
|---|---|---|
| `collection_date` | DATE | `date(data_interval_end)`, партиция |
| `event_date` | DATE | `date(fired_at)` |
| `fired_at` | TIMESTAMP | `fired_at` |
| `model_name` | VARCHAR | `model_name` |
| `request_id` | VARCHAR | `request_id` |
| `install_id` | VARCHAR | `install_id` |
| `search_query` | VARCHAR | `search_query` |
| `category_id` | INT | `category_id` |
| `promo_id` | VARCHAR | `promo_id` |
| `position` | INT | индекс кандидата в массиве, 1-based |
| `sku_group_id` | BIGINT | `ranking_candidates[position]` |
| `final_score` | DOUBLE | `final_scores[position]` |
| `model_probability` | DOUBLE | `model_output[position][2]` |
| `alpha_component` | DOUBLE | `model_output[position][3]` |
| `beta_component` | DOUBLE | `model_output[position][4]` |
| `gamma_component` | DOUBLE | `model_output[position][5]` |
| `delta_component` | DOUBLE | `model_output[position][6]` |
| `dssm_score` | DOUBLE | `external_features.dssm_score[position]` |
| `linear_score` | DOUBLE | `external_features.linear_score[position]` |
| `normalized_linear_score` | DOUBLE | `external_features.normalized_linear_score[position]` |
| `cpo_adv_percent` | DOUBLE | `external_features.cpo_adv_percents[position]` |
| `bid_amount` | DOUBLE | `external_features.bid_amounts[position]` |
| `commission_percent` | DOUBLE | `cm2_features[position][1]` |
| `seller_price` | DOUBLE | `cm2_features[position][2]` |
| `logistics_fee` | DOUBLE | `cm2_features[position][3]` |
| `cpi_cost` | DOUBLE | `cm2_features[position][4]` |
| `cpm_bid` | DOUBLE | `cm2_features[position][5]` |
| `cpo_percent` | DOUBLE | `cm2_features[position][6]` |
| `vat_rate` | DOUBLE | `cm2_features[position][7]` |
| `items_quantity` | DOUBLE | `cm2_features[position][8]` |
| `alpha` | DOUBLE | `common_external_features['alpha']` |
| `beta` | DOUBLE | `common_external_features['beta']` |
| `gamma` | DOUBLE | `common_external_features['gamma']` |
| `delta` | DOUBLE | `common_external_features['delta']` |
| `sku_group_age_days` | INT | обогащение, см. 5.1 |
| `product_rating` | DOUBLE | обогащение, см. 5.2 |
| `total_reviews_count` | BIGINT | обогащение, см. 5.2 |
| `frequency_group` | VARCHAR | обогащение, см. 5.3 |
| `users_total` | BIGINT | обогащение, см. 5.3 |
| `query_rank` | BIGINT | обогащение, см. 5.3 |

### Решения по схеме

- **`model_input['input']` (145 признаков) не пишется.** Имён признаков в логе нет, а для
  подбора параметров формулы нужны только её составляющие. Это же решение держит строку узкой
  при 200 млн строк на ран.
- **`model_output[position][1]` не пишется отдельной колонкой** — он равен `final_scores[position]`.
  В датасете остаётся один `final_score`; равенство подтверждается DQ-тестом.
- **`alpha`/`beta`/`gamma`/`delta` пишутся один раз.** В источнике они лежат и в
  `common_external_features`, и в хвосте `cm2_features` (позиции 9–12) с одинаковыми значениями;
  берётся `common_external_features` как явно request-level контракт.
- **`silver.sku_eod` не используется.** Цена берётся из лога (`cm2_features[2]`, «цена продажи»):
  формула считалась именно на ней, а не на средней по sku-группе. `silver.sku` остаётся только
  ради `created_at` для возраста.

## 5. Обогащения

### 5.1 Возраст sku-группы

```
sku_group_age_days = date_diff('day', date(min(sku.created_at)), event_date)
```

Агрегация `min(created_at)` по `sku_group_id` над `silver.sku` — возраст самого старого sku
в группе. `silver.sku` — снапшот без истории, поэтому для удалённых sku возраст будет неполным;
это принятое ограничение, а не дефект.

### 5.2 Рейтинг

Источник — `iceberg.gold.feature_platform_sku_group_feedback_base_stats`, join по
`sku_group_id` и `date = event_date`. Берутся `product_rating` и `total_reviews_count`
(рейтинг без объёма отзывов интерпретируется неверно).

Если партиция за `event_date` отсутствует, берётся последняя доступная `date <= event_date`.
Наличие ежедневных партиций проверяется на этапе реализации.

### 5.3 Частотность запроса

Источник — `"dwh-iceberg".silver.search_queries_frequency_groups_30d`, join по
`lower(trim(search_query)) = lower(trim(query_text))`, `analyze_date` — последняя доступная
`<= event_date`. Берутся `frequency_group`, `users_total`, `query_rank`.

При отсутствии совпадения `frequency_group = 'LF'`, `users_total` и `query_rank` = NULL.
Это безопасный дефолт: на 2026-08-31 группа LF содержала 12,6 млн запросов против
9013 в MF и 1000 в HF, поэтому ненайденный запрос почти наверняка низкочастотный.

## 6. Качество и наблюдаемость

Граф DAG'а: `wait_for_sku_group_feedback >> collect_ranking_logs_dataset >> [dq_task, stats_task]`.
DQ и feature_stats идут параллельно и на цену скана источника не влияют — обе таски читают уже
записанную партицию целевой таблицы через Trino.

`wait_for_sku_group_feedback` — `ExternalTaskSensor` на таску `dq` DAG'а
`feature-platform.layers.gold.sku_group_id.feedback_sku_group_id`, владельца
`iceberg.gold.feature_platform_sku_group_feedback_base_stats` (раздел 5.2). Обязателен по
AGENTS.md: «датасет, читающий repository-managed layer/dataset таблицу, обязан объявить
сенсор на `dq`-таску DAG'а-владельца» — то, что этот датасет offline-only, не освобождает
от сенсора. Это не формальность: `feedback` CTE берёт последнюю партицию `<= event_date` с
30-дневным окном отката, поэтому отсутствие свежей партиции не роняет джоб — оно молча
подставляет более старый снапшот рейтинга. Сенсор превращает эту тихую деградацию в
видимое ожидание вместо тихой порчи данных.

Остальные три источника (`silver.ranking_analytics_events`, `silver.sku`,
`silver.search_queries_frequency_groups_30d`) — upstream DE-таблицы без префикса
`feature_platform_`, сенсоры на них не нужны.

`feedback_sku_group_id` идёт по `10 3 * * * UTC` и пишет партицию `date = data_interval_start`.
Наше окно открывается в `logical_date` (воскресенье) и закрывается субботой (`logical_date + 6
дней`) — то есть нужный прогон feedback имеет логическую дату позже нашей, что не выражается
положительным `execution_delta`; отсюда `execution_date_fn=_feedback_dq_logical_date`.

### DQ

`trino_conn_id: trino_search`, партиция и шаблон — как в разделе 2.

Все тесты в severity `warn`, по действующей политике репозитория: это датасет событийных
показов, где первичный ключ задаёт гранулярность строки, а не контракт для потребителей,
и качество источника не должно будить дежурного. Результаты по-прежнему пишутся в
`feature_platform_dq_results`.

| Тест | Severity | Комментарий |
|---|---|---|
| `primary_key_not_null` | warn | |
| `primary_key_unique` | warn | дублей `sku_group_id` внутри запроса в источнике нет — проверено на 1098 запросах (919 022 кандидата, `array_distinct` даёт ту же длину) |
| `freshness` | warn | |
| `row_count_min` | warn | порог по факту первых ранов |
| `row_count_growth` | disabled | явно отключен через `enabled: false` в config.yaml, так как `dq/tests.py` жёстко берёт baseline как `partition_date - 1 day`, для недельной партиции это не работает |
| `not_null` на `final_score`, `sku_group_id` | warn | |

### feature_stats

`trino_conn_id: trino_search`, партиция и шаблон дословно совпадают с `dq`.

`exclude_columns`: `sku_group_id`, `request_id`, `install_id`, `promo_id`, `search_query` —
идентификаторы и текст, профиль min/max/перцентилей по ним бессмыслен.

## 7. Проверить на этапе реализации

1. `cm2_features.cpo_percent` против `external_features.cpo_adv_percents` и
   `cm2_features.cpm_bid` против `external_features.bid_amounts`: если это одни и те же
   величины, дублирующие колонки убираются из схемы.
2. Наличие ежедневных партиций `feature_platform_sku_group_feedback_base_stats`; при пропусках
   применяется fallback «последняя `date <= event_date`» из 5.2.
3. Фактические ресурсы Spark на первом ране и необходимость отдельного профиля.
