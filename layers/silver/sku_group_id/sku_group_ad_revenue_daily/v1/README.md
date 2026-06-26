# Silver Ad Revenue Daily по SKU Group ID

DAG id: `feature-platform.layers.silver.sku_group_id.sku_group_ad_revenue_daily`.

Пайплайн собирает дневной silver pre-aggregate рекламной выручки на уровне `sku_group_id`.

Целевая таблица: `iceberg.silver.feature_platform_sku_group_ad_revenue_daily`.

Грейн: `date, sku_group_id` (первичный ключ). Одна строка на `sku_group_id` за дату `ds`.

## Источник

- `iceberg.silver.adv_funnel_daily` — дневной рекламный funnel из paid-promo (зеркало `paid_promo.adv_funnel_daily`). DE-owned upstream-таблица, feature-platform её не производит.

Важные особенности источника:

- Числовые поля (`impressions`, `clicks`, `adrev`, ...) хранятся как `varchar`, поэтому в job они приводятся к числовым типам явным `CAST`.
- `adrev` — это рекламные расходы продавца, то есть заработок платформы с рекламы.
- В источнике **нет разреза по площадке** (`widget_space_name`/`SEARCH_RESULTS`). Это весь рекламный CPC-funnel продвижения (поисковая выдача + категории вместе). Выделить строго «витрину поиска» (`SEARCH_RESULTS`) из этого источника невозможно; для строго-поисковой атрибуции потребовалась бы отдельная DE-ingestion event-level данных (`adv_cpo_golden_events`) в грейн `sku_group × widget_space`.

## Сбор

- Аггрегируется ровно один день — партиция `ds`: `WHERE date = DATE(ds)`.
- Исключаются строки с пустым/нулевым `sku_group_id`.
- Запись идёт через `overwritePartitions()` по партиции `date`, поэтому каждый запуск перезаписывает только свой день, а история накапливается для оконных gold-расчётов.

## Колонки

- `date` — дата партиционирования (= `ds`).
- `sku_group_id` — ID sku group.
- `ad_impressions` — сумма рекламных показов за день.
- `ad_clicks` — сумма кликов по рекламе за день.
- `ad_revenue` — сумма `adrev` за день (заработок платформы с рекламы sku group).

## Runtime

Дефолтный Spark image + `git-sync` initContainer. Код запускается из
`/git/repo/layers/silver/sku_group_id/sku_group_ad_revenue_daily/v1/entrypoints/get_sku_group_ad_revenue_daily.py`,
отдельный Docker image не собирается.
