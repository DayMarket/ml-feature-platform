# Silver Daily linear_score по Query/SKU Group

Пайплайн собирает дневной средний `linear_score` и `normalized_linear_score`
на уровне `date`, `query`, `sku_group_id`.

Целевая таблица: `iceberg.silver.feature_platform_query_skg_daily_linear_score`.

Источник: `iceberg.silver.ranking_analytics_events` (DE-овнерская таблица).

Основная логика:

- читает события за интервал `{{ ds }} 00:00:00` - `{{ next_ds }} 00:00:00`;
  границы суток — UTC (`spark.sql.session.timeZone=UTC`), дата берётся как
  `to_date(fired_at)`;
- фильтрует модели `model_name LIKE 'search_unified_model_v%'` — только для них
  присутствует `linear_score`;
- `ranking_candidates` уже содержит `sku_group_id` (маппинг через `silver.sku`
  не нужен);
- достаёт массивы `linear_score` и `normalized_linear_score` из JSON-поля
  `external_features`; массивы позиционно выровнены с `ranking_candidates`;
- через `arrays_zip` + `explode` разворачивает в строки
  `(sku_group_id, linear_score, normalized_linear_score)`;
- нормализует запрос как в query×skg фичах: `lower` → `ё`→`е` →
  схлопывание пробелов (`\s+` → ` `) → `trim`, отбрасывает пустые;
- агрегирует по `date, query, sku_group_id`:
  `avg_linear_score`, `avg_normalized_linear_score`, `observations`
  (число усреднённых позиций);
- `avg` по Spark-семантике игнорирует null-элементы; пары без валидных скоров
  за день не пишутся.

Расписание: `0 1 * * *`. Внешнего сенсора нет (источник DE-овнерский,
полагаемся на расписание). Downstream-выгрузки в ranking нет (silver-предагрегат).
