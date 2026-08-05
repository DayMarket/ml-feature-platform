# Handoff: S1 `product_attributes_snapshot` в `ml-feature-platform`

## Задача

Нужно завершить перенос первого Silver-контракта миграции recsys-фич в основной репозиторий:

`/Users/a.b.latipov/PycharmProjects/ml-feature-platform`

Работать нужно непосредственно в этом checkout. Не оставлять итоговую реализацию только в snapshot или вспомогательном клоне.

Ветка должна называться:

`recsys-s1-product-attributes-snapshot`

Push делать не нужно.

## Что уже готово

Проверенная реализация находится в локальном Git-клоне:

`/Users/a.b.latipov/Documents/Codex/2026-07-23/recsys-dag-recsys-dag-ml-feature-2/work/ml-feature-platform-s1`

Ветка:

`recsys-s1-product-attributes-snapshot`

Коммит:

`734cc6b4ca937257d29300fd50cd42ee9a96d3b4`

Сообщение коммита:

`feat: add recsys S1 product attributes snapshot`

Коммит основан на `master` с SHA `7cb2bd5`.

В основном checkout локальная ветка `recsys-s1-product-attributes-snapshot` уже могла быть создана, но до переноса она указывала на `7cb2bd5`, то есть не содержала S1.

После переноса реализация была дополнительно пересмотрена в основном checkout:

- допущения об уникальном grain upstream-справочников и невозможности честного исторического rerun приняты;
- L6 определён как последний содержательный элемент пути и проверяется как обязательный;
- проверки L6 и глубины иерархии оставлены локальными для producer job;
- SQL переведён с дублирующих констант на runtime-секцию `source`;
- SQL вынесен в чистый query builder и покрыт тестами по принятому в репозитории паттерну.

## Как перенести готовый коммит

Сначала проверить, что в основном checkout нет пользовательских изменений:

```bash
git status --short --branch
```

Если worktree чистый:

```bash
git fetch \
  /Users/a.b.latipov/Documents/Codex/2026-07-23/recsys-dag-recsys-dag-ml-feature-2/work/ml-feature-platform-s1 \
  734cc6b4ca937257d29300fd50cd42ee9a96d3b4

git switch recsys-s1-product-attributes-snapshot
git merge --ff-only FETCH_HEAD
```

После переноса:

```bash
git log -1 --oneline
git status --short --branch
git diff master...HEAD --stat
```

Ожидаемый HEAD:

`734cc6b feat: add recsys S1 product attributes snapshot`

Не нужно заново переписывать реализацию до просмотра готового diff. Если актуальный `master` ушёл вперёд, сначала оценить расхождение и аккуратно адаптировать код по действующим правилам репозитория.

## Назначение S1

S1 — переиспользуемый point-in-time Silver-справочник атрибутов товара для последующего расчёта recsys Gold-фич.

Целевая таблица:

`iceberg.silver.feature_platform_product_attributes_snapshot`

Путь контракта:

`layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1`

Grain и primary key:

`snapshot_date × product_id`

`snapshot_date_product_id` используется в пути сущности намеренно: текущая CI-логика репозитория выводит primary-key group из полного primary key.

## Схема контракта

| Колонка | Смысл |
|---|---|
| `snapshot_date` | Локальная дата snapshot в `Asia/Tashkent` |
| `product_id` | ID товара |
| `l1_category_id` | Первый содержательный уровень иерархии |
| `l2_category_id` | L2 либо ближайший существующий предыдущий уровень |
| `l3_category_id` | L3 либо ближайший существующий предыдущий уровень |
| `l4_category_id` | L4 либо ближайший существующий предыдущий уровень |
| `l5_category_id` | L5 либо ближайший существующий предыдущий уровень |
| `l6_category_id` | Последний содержательный элемент пути; конечная `product.category_id` |
| `brand_id` | Канонический содержательный бренд |
| `shop_id` | Магазин товара |
| `created_at` | Время создания товара в UTC |
| `category_gender` | `M`, `F`, `U` или `NULL` |

`age_in_days` и отдельные gender-флаги в S1 не хранятся. Они должны вычисляться в Gold относительно его `calculated_at`.

## Источники

Используются:

- `iceberg.silver.product`;
- `iceberg.silver_apidb_kazanexpress.public_category`;
- `iceberg.silver.sku`;
- `iceberg.silver.recsys_category_genders`.

`iceberg.silver.category` не использовать: во время проверки через Trino такой таблицы не было. Категорийная иерархия восстанавливается из `silver_apidb_kazanexpress.public_category`.

## Зафиксированная логика

### Категории

- путь строится от конечной категории к корню;
- технический root `category_id = 1` исключается;
- максимальная поддерживаемая глубина — 10;
- job должен завершаться ошибкой, если встречается более глубокий путь;
- L1 — первый содержательный уровень;
- L2–L5 заполняются ближайшим предыдущим содержательным уровнем, если отдельного уровня нет;
- L6 равен последнему ненулевому содержательному элементу пути, то есть конечной `product.category_id`;
- отсутствие содержательного L6 считается нарушением: job завершается ошибкой до записи.

### Brand

- `brand_name_id = 160078` является business placeholder и исключается;
- `NULL` также исключается;
- при нескольких допустимых brand у одного product выбор должен быть детерминированным;
- согласованный anomaly fallback — `MIN(brand_name_id)`.

При проверке источника было найдено 19 product с несколькими содержательными brand. Поэтому нельзя возвращать недетерминированный `DISTINCT ON` без `ORDER BY`.

### Gender категории

- join выполняется по конечной `product.category_id`;
- источник — `iceberg.silver.recsys_category_genders`;
- допустимые значения: `M`, `F`, `U`;
- прочие значения приводятся к `NULL`.

## Оркестрация

DAG id:

`feature-platform.layers.silver.snapshot_date_product_id.product_attributes_snapshot`

Airflow group tag:

`recsys-main-page-features`

Расписание:

`0 19 * * *`

Это ежедневный запуск в `19:00 UTC`, то есть в `00:00 Asia/Tashkent`.

Start date:

`2026-08-04T19:00:00Z`

`catchup=False`.

Причина отсутствия catchup: источники содержат текущее состояние и не позволяют честно восстановить исторический point-in-time snapshot.

`snapshot_date` определяется как локальная дата `data_interval_end` в `Asia/Tashkent`. Например, `2026-08-04 19:00:00 UTC` публикует `snapshot_date = 2026-08-05`.

Повторный запуск перезаписывает только целевую партицию через `overwritePartitions()`.

Resource profile:

`medium`

Отдельный Spark image не создаётся.

Upstream DQ DAG ids не были известны, поэтому отдельные sensors согласованно не добавлялись.

Для результирующего source генерируются только стандартные DQ-проверки:
уникальность ключа и `not_null` его колонок. Проверки L6 и глубины иерархии
выполняет сам S1 job до записи; остальные допустимые значения обеспечиваются
трансформацией.

Ranking upload не нужен: S1 — внутренний Silver-контракт. Не нужно добавлять `ranking_service_input.yaml` только ради прохождения `scripts/validate_ranking_upload_configs.py`.

## Файлы готовой реализации

- `ci_test/test_product_attributes_snapshot.py`;
- `layers/silver/README.md`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/README.md`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/config.yaml`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/config/factory.py`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/dag.py`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/entrypoints/get_product_attributes_snapshot.py`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/job/__init__.py`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/job/arguments.py`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/job/entities.py`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/job/getting_product_attributes_snapshot.py`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/job/partition.py`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/job/query.py`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/job/runtime_config.py`;
- `layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1/migrations/create_table.sql`.

## Проверки

В рабочем клоне и в файловом snapshot уже проходили:

```bash
UV_CACHE_DIR=/tmp/codex-uv-cache-s1 \
UV_TOOL_DIR=/tmp/codex-uv-tools-s1 \
UV_TOOL_BIN_DIR=/tmp/codex-uv-bin-s1 \
PYTHONDONTWRITEBYTECODE=1 \
uvx --from pytest pytest ci_test/test_product_attributes_snapshot.py -q
```

Результат:

`14 passed`

Lint:

```bash
UV_CACHE_DIR=/tmp/codex-uv-cache-s1 \
UV_TOOL_DIR=/tmp/codex-uv-tools-s1 \
UV_TOOL_BIN_DIR=/tmp/codex-uv-bin-s1 \
uvx ruff check \
  layers/silver/snapshot_date_product_id/product_attributes_snapshot/v1 \
  ci_test/test_product_attributes_snapshot.py
```

Результат:

`All checks passed`

Расширенный целевой набор для S1, migration helpers и dbt source sync:

```bash
UV_CACHE_DIR=/tmp/codex-uv-cache-s1 \
UV_TOOL_DIR=/tmp/codex-uv-tools-s1 \
UV_TOOL_BIN_DIR=/tmp/codex-uv-bin-s1 \
PYTHONDONTWRITEBYTECODE=1 \
uvx --with pyyaml --from pytest pytest \
  ci_test/test_product_attributes_snapshot.py \
  ci_test/test_script.py \
  ci_test/test_run_pyspark_migrations.py \
  ci_test/test_sync_dbt_sources.py -q
```

Результат после ревью: `14 passed`.

После переноса обязательно повторить проверки в основном checkout.

Для общего CI можно использовать:

```bash
UV_CACHE_DIR=/tmp/codex-uv-cache-s1 \
UV_TOOL_DIR=/tmp/codex-uv-tools-s1 \
UV_TOOL_BIN_DIR=/tmp/codex-uv-bin-s1 \
uvx --with pyyaml --with joblib --from pytest pytest ci_test -q
```

На исходном `master` уже наблюдались не связанные с S1 проблемы:

- пять Geo-тестов;
- две ошибки ссылок Gold README в `ci_test/test_script.py`.

Если они повторяются, сначала воспроизвести их на `master`, а не исправлять в рамках S1 без подтверждения связи.

После ревью общий pytest показал `90 passed` и те же пять Geo-падений.
`python3 ci_test/test_script.py` показал те же две ошибки ссылок Gold README.
Проверка Spark/Iceberg migrations локально не запускалась: в checkout нет Java
runtime; она остаётся обязательным containerized Drone-шагом.

## Что нужно сделать новому агенту

1. Прочитать `AGENTS.md` и актуальные правила основного checkout.
2. Проверить чистоту worktree.
3. Перенести готовый коммит в основную ветку `recsys-s1-product-attributes-snapshot`.
4. Просмотреть полный diff относительно актуального `master`.
5. Проверить соответствие контракта требованиям выше.
6. Исправить только реальные проблемы S1.
7. Запустить целевой pytest и Ruff.
8. По возможности запустить общий CI и отделить baseline failures.
9. Убедиться, что нет `__pycache__`, `.pyc` и незакоммиченных изменений.
10. Не делать push.
11. В финале сообщить SHA, список файлов и результаты проверок.

## Дополнительные материалы

Финальный каталог recsys-фич:

`/Users/a.b.latipov/Downloads/Переезд рексис фичей на ml-feature-platform.docx`

Рабочая копия каталога:

`/Users/a.b.latipov/Documents/Codex/2026-07-23/recsys-dag-recsys-dag-ml-feature-2/outputs/recsys-feature-catalog.docx`

Для завершения S1 весь каталог читать не обязательно: зафиксированные требования S1 приведены в этом handoff. Каталог понадобится для проверки потребителей и для дальнейшей реализации S2–S6 и Gold-контрактов.
