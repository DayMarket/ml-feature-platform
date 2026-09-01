# ranking_logs v1

Тренировочный датасет для офлайн-подбора параметров ранжирующей формулы поиска.
Разворачивает лог ранжирующего сервиса до уровня «запрос × кандидат» и добавляет
разложение формулы, её входы и внешние скоры.

- Таблица: `iceberg.silver.feature_platform_ranking_logs_dataset_v1`
- DAG: `feature-platform.datasets.search.ranking_logs.v1`
- Group tag: `ranking-logs-dataset`
- Путь энтити: `datasets/search/ranking_logs/v1`
- Primary key: `collection_date, event_date, request_id, sku_group_id`. Грейн —
  одна строка на кандидата сэмплированного запроса (запрос × кандидат).
- Назначение: офлайн-подбор параметров формулы и анализ. В ranking-service,
  inference-сервисы и любой онлайн-контур не выгружается.

## Окно и партиция

DAG идёт раз в неделю, `0 12 * * 0` UTC. `data_interval` недельный, поэтому один
ран покрывает 7 календарных суток:

- `event_date ∈ [date(data_interval_start), date(data_interval_end) - 1]` —
  воскресенье…суббота включительно, последние закрытые сутки — вчерашние;
- `collection_date = date(data_interval_end)` — воскресенье фактического запуска,
  она же партиция таблицы.

`collection_date` считается от `data_interval_end`, а не от `data_interval_start`
как в `datasets/search/search_ranking/v1`. Шаблоны `dq` и `feature_stats`
используют тот же `data_interval_end`.

## Отбор

Одна модель на ран, имя в `config.yaml` → `dataset.model_name`, сейчас
`search_unified_model_v9_cold_start`.

Сэмплирование детерминированное и по запросу, а не по строке-кандидату:
`pmod(xxhash64(request_id), 10000) < dataset.sample_percent * 100`. Попавший в
выборку запрос берётся целиком, со всеми кандидатами, без обрезки по позиции.
Перезапуск за ту же неделю даёт ту же выборку.

Стратификации по `frequency_group` нет сознательно: случайный отбор по запросам
сохраняет долю HF/MF/LF такой, какая она в трафике, а формула подбирается именно
под реальный трафик. `frequency_group` остаётся колонкой, стратифицировать можно
на анализе.

Объём: ~837 кандидатов на запрос, ~482 тыс. запросов в сутки по целевой модели,
то есть при плановых `sample_percent = 7` порядка 28 млн строк в сутки и ~200 млн
за ран.

На первый ран `dataset.sample_percent` в `config.yaml` намеренно установлен в **1**:
~4 млн строк в сутки и ~28 млн за ран. Первый ран разведочный — его задача измерить
реальные метрики Spark, а не собрать боевой объём. Плановая доля 7% возвращается
после этого измерения, вместе с пересмотром профиля ресурсов `search_dataset`
(напомним: `model_input` в источнике — `map`, ключ из него не проецируется, поэтому
145-мерный вектор читается при каждом ране, сколько бы строк ни писалось).

## Источники

| Что | Откуда |
|---|---|
| Лог ранжирования | `iceberg.silver.ranking_analytics_events` |
| Возраст sku group | `iceberg.silver.sku`, `min(created_at)` по `sku_group_id` |
| Рейтинг и отзывы | `iceberg.gold.feature_platform_sku_group_feedback_base_stats` |
| Частотность запроса | `iceberg.silver.search_queries_frequency_groups_30d` |

`feature_platform_sku_group_feedback_base_stats` — repository-managed таблица,
владелец DAG `feature-platform.layers.gold.sku_group_id.feedback_sku_group_id`.
Наш DAG ждёт его таску `dq` сенсором `wait_for_sku_group_feedback` (AGENTS.md,
раздел про DQ-сенсоры) до `collect_ranking_logs_dataset`; остальные три
источника — upstream DE-таблицы без `feature_platform_`-префикса и сенсоров не
требуют.

Все массивы источника выровнены 1:1 с `ranking_candidates` — проверено на 7424
событиях. Разворот идёт общим `posexplode(arrays_zip(...))`.

`model_input['input']` (145 признаков) в датасет не пишется: имён признаков в
логе нет, а для подбора параметров формулы нужны только её составляющие.
`model_output[0]` тоже не пишется отдельной колонкой — он равен
`final_scores[position]`.

`model_input` в источнике — `MAP(VARCHAR, ARRAY(ARRAY(DOUBLE)))`, а не STRUCT
(подтверждено на живой схеме). Parquet не умеет проецировать отдельное значение
MAP-колонки, поэтому то, что датасет не пишет `model_input['input']`, сужает
только **выходную** строку — на сам скан это не влияет: каждый ран читает весь
`model_input` целиком, включая 145-мерный `input`, из
`iceberg.silver.ranking_analytics_events`. Профиль Spark `search_dataset` взят
как стартовый (design doc, раздел 3) и должен быть пересмотрен по факту первого
рана с учётом этого факта.

`alpha`/`beta`/`gamma`/`delta` лежат в источнике дважды — в
`common_external_features` и в хвосте `cm2_features` (позиции 8–11) с теми же
значениями. Берётся `common_external_features` как явный request-level контракт.

Цена берётся из лога (`cm2_features[1]`, цена продажи), а не как средняя по
sku-группе из `silver.sku_eod`: формула считалась именно на цене из лога.

Если частотный справочник не знает запрос, `frequency_group = 'LF'`, а
`users_total` и `query_rank` — NULL. Это безопасный дефолт: на 2026-08-31 в LF
было 12,6 млн запросов против 9013 в MF и 1000 в HF.

## Leakage boundary и обработка сломанных строк лога

Две колонки одной строки живут в разных временных контрактах, и это не видно без
чтения `job/query.py`:

- `sku_group_age_days` считается по **текущему, неисторическому** снапшоту
  `silver.sku` (`MIN(created_at)` по `sku_group_id`, без версии на дату),
  применённому к событиям недельной давности: для `event_date` из середины
  собираемого окна возраст вычисляется относительно состояния `silver.sku` на
  момент сбора, а не на сам `event_date`.
- `product_rating` и `total_reviews_count`, напротив, берутся из **последнего
  доступного** снапшота `feature_platform_sku_group_feedback_base_stats` с
  `date <= event_date` — честный контракт «как было на дату события».

Подбор параметров формулы не должен интерпретировать `sku_group_age_days` как
признак «на момент события» наравне с рейтингом: это разные temporal-контракты
в одной строке.

Джоб также по-разному реагирует на рассогласование длины массива-кандидата в
зависимости от того, откуда массив взят:

- Рассогласование длины **нативного** массива источника (`final_scores`,
  `model_output`, `model_input['cm2_features']`) с `ranking_candidates` —
  событие **выбрасывается целиком**, это `WHERE`-guard'ы в `sampled_events`
  (`job/query.py`).
- Рассогласование длины массива, **раскодированного из JSON**
  (`external_features.dssm_score`, `.linear_score`,
  `.normalized_linear_score`) — строка
  лога сохраняется, а сама колонка **обнуляется** для всех кандидатов этого
  события, это `CASE`-блоки в `sampled_events`.

Оба режима — молчаливая потеря данных: сколько событий выброшено и сколько
колонок обнулено, нигде не считается и не проверяется DQ-тестом.

## Качество

Все DQ-тесты в severity `warn` по политике репозитория для событийных датасетов:
первичный ключ здесь задаёт гранулярность строки, а не контракт для
потребителей. Результаты пишутся в `feature_platform_dq_results`.

`row_count_growth` явно отключен через `enabled: false`: `dq/tests.py` берёт
baseline как `partition_date - 1 day`, у недельной партиции предыдущих суток не
существует, и тест на каждом ране возвращал бы «нет baseline». Явное отключение
через `enabled: false` необходимо, потому что `dq/config.py` инъецирует базовые
тесты в разрешённые параметры: любой базовый тест, отсутствующий из явного
списка, получает severity `error` по умолчанию.

`feature_stats` считает профиль по той же партиции; идентификаторы и текст
(`sku_group_id`, `request_id`, `install_id`, `promo_id`, `search_query`)
исключены из профиля.
