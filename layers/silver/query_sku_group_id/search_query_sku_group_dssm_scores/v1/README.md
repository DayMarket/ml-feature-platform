# Search Query/SKU Group DSSM Scores

Пайплайн собирает silver-признак `dssm_score` из ranking analytics logs на уровне поискового запроса и
`sku_group_id`.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_search_query_sku_group_dssm_scores`.
- DAG: `feature-platform.layers.silver.query_sku_group_id.search_query_sku_group_dssm_scores`.
- Путь: `layers/silver/query_sku_group_id/search_query_sku_group_dssm_scores/v1`.
- Групповой тег Airflow: `search-query-sku-dssm`.
- Расписание: ежедневно в 02:00 UTC, `0 2 * * *`.
- `start_date=2026-03-01T00:00:00Z`, `catchup=True`.

## Грейн / ключ

Primary key: `date, query, sku_group_id, collected_at`.

`date` - UTC-дата дневного окна, равная `data_interval_start::date`. DAG запускается в 02:00 UTC, но
читает календарный день UTC от полуночи до полуночи.

`collected_at` входит в ключ осознанно: DSSM score считается почти стабильным, но источник может переотдать
другое значение для той же пары `date, query, sku_group_id`. Если новая версия отличается от последней
записанной версии минимум на 5%, пайплайн добавляет новую строку, а старую не удаляет. Физически сущность
лежит в группе `query_sku_group_id`, потому что `collected_at` является технической версией, а не частью
бизнес-грейна.

## Источник

- `iceberg.silver.ranking_analytics_events` - внешний Iceberg-источник ranking analytics events.

Контракт источника зафиксирован в задаче: `ranking_candidates` трактуется как массив `sku_group_id`,
`external_features` содержит DSSM score по JSON path `$.dssm_score`, а поисковый срез выбирается по
`model_name LIKE '%search_uni%'`.

## Логика

Каждый DAG run читает закрытое UTC-окно для даты `data_interval_start::date`:

- `fired_at >= date 00:00:00 UTC`;
- `fired_at < date + 1 day 00:00:00 UTC`.

Из событий берутся непустые `search_query`, непустые `ranking_candidates` и непустой `external_features`.
`search_query` пишется в колонку `query` без нормализации.

`dssm_score` извлекается из `external_features` по `$.dssm_score`. Если значение является массивом той же длины,
что и `ranking_candidates`, scores сопоставляются с кандидатами по позиции. Если значение является скаляром,
оно применяется ко всем `ranking_candidates` события. События с непарсируемым или `NULL` score не пишутся.

Для одной пары `date, query, sku_group_id` внутри дневного окна берется latest событие по `fired_at DESC`.
Затем результат `LEFT JOIN`-ится к последней уже записанной версии этой пары в целевой таблице:

- если старой версии нет или старый `dssm_score` равен `NULL`, строка добавляется;
- если старый score равен `0`, новая версия добавляется только при ненулевом новом score;
- иначе новая версия добавляется при `abs(new - old) / abs(old) >= 0.05`.

Прямые дубли при retry или повторном backfill не накапливаются: если latest source score совпадает с последней
записанной версией с точностью до 5%, строка не append-ится.

## Оценка особенности

Append-on-change модель подходит, если DSSM score является внешним near-static feature и важно сохранить факт
изменения источника без destructive overwrite. Цена подхода - у потребителя всегда должна быть логика выбора
последней версии по `collected_at`, иначе для одной пары могут появиться несколько строк. Для online upload этот
silver-слой сам по себе не меняет текущий `EXTERNAL dssm_score` контракт ranking-service.

## DQ

`table.meta.create_dbt_pr: false`, поэтому CI не создает dbt-trino source/DQ PR и dbt-тесты для этой витрины.
Отдельные проверки распределения `dssm_score` не добавлены: значение приходит из внешнего ranking analytics
contract и может меняться вместе с upstream DSSM расчетом.

## Рантайм

Spark/Iceberg pipeline через общий Spark image и `git-sync`, отдельный Docker image не собирается.
Spark resource profile: `small`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
