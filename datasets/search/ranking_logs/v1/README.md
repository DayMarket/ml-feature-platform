# ranking_logs v1

Тренировочный датасет для офлайн-подбора параметров ранжирующей формулы поиска.
Разворачивает лог ранжирующего сервиса до уровня «запрос × кандидат» и добавляет
разложение формулы, её входы и внешние скоры.

- Таблица: `iceberg.silver.feature_platform_ranking_logs_dataset_v1`
- DAG: `feature-platform.datasets.search.ranking_logs.v1`
- Путь энтити: `datasets/search/ranking_logs/v1`
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
то есть при `sample_percent = 7` порядка 28 млн строк в сутки и ~200 млн за ран.

## Источники

| Что | Откуда |
|---|---|
| Лог ранжирования | `iceberg.silver.ranking_analytics_events` |
| Возраст sku group | `iceberg.silver.sku`, `min(created_at)` по `sku_group_id` |
| Рейтинг и отзывы | `iceberg.gold.feature_platform_sku_group_feedback_base_stats` |
| Частотность запроса | `iceberg.silver.search_queries_frequency_groups_30d` |

Все массивы источника выровнены 1:1 с `ranking_candidates` — проверено на 7424
событиях. Разворот идёт общим `posexplode(arrays_zip(...))`.

`model_input['input']` (145 признаков) в датасет не пишется: имён признаков в
логе нет, а для подбора параметров формулы нужны только её составляющие.
`model_output[0]` тоже не пишется отдельной колонкой — он равен
`final_scores[position]`.

`alpha`/`beta`/`gamma`/`delta` лежат в источнике дважды — в
`common_external_features` и в хвосте `cm2_features` (позиции 8–11) с теми же
значениями. Берётся `common_external_features` как явный request-level контракт.

Цена берётся из лога (`cm2_features[1]`, цена продажи), а не как средняя по
sku-группе из `silver.sku_eod`: формула считалась именно на цене из лога.

Если частотный справочник не знает запрос, `frequency_group = 'LF'`, а
`users_total` и `query_rank` — NULL. Это безопасный дефолт: на 2026-08-31 в LF
было 12,6 млн запросов против 9013 в MF и 1000 в HF.

## Качество

Все DQ-тесты в severity `warn` по политике репозитория для событийных датасетов:
первичный ключ здесь задаёт гранулярность строки, а не контракт для
потребителей. Результаты пишутся в `feature_platform_dq_results`.

`row_count_growth` в конфиге отсутствует: `dq/tests.py` берёт baseline как
`partition_date - 1 day`, у недельной партиции предыдущих суток не существует,
и тест на каждом ране возвращал бы «нет baseline».

`feature_stats` считает профиль по той же партиции; идентификаторы и текст
(`sku_group_id`, `request_id`, `install_id`, `promo_id`, `search_query`)
исключены из профиля.
