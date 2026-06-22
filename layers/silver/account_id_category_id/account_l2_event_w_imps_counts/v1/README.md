# account_l2_event_w_imps_counts

Silver-пайплайн строит дневную предагрегацию событий на уровне пары `account_id` и L2 `category_id`.

## Выходная таблица

- `iceberg.silver.feature_platform_account_l2_event_w_imps_counts`
- grain и primary key: `date, account_id, category_id`
- партиционирование: `date`

## Источники

- `iceberg.silver_b2c_clickstream.events` - события за дату расчета в таймзоне `Asia/Tashkent`;
- `iceberg.silver.product` - fallback-маппинг `product_id` в `category_id`, если в событии `category_id <= 0`;
- `iceberg.silver.category` - иерархия категорий `l1_category`, `l2_category`, `l3_category`.

## Логика

Пайплайн учитывает только `account_id > 0` и события `PRODUCT_IMPRESSION`, `PRODUCT_VIEW`, `ADD_TO_CART`, `ADD_TO_FAVORITES`.

Для каждого события выбирается категория: `event.category_id`, если она больше нуля, иначе `category_id` товара из `iceberg.silver.product`. Затем категория приводится к L2 через справочник категорий. Если L2 отсутствует или меньше/равна нулю, используется L1.

Метрики повторяют ClickHouse-семантику `uniqIf(product_id, event_type = ...)` внутри `(account_id, session_id, category_id)`, после чего суммируются до `(account_id, category_id)`:

- `n_imps` - кол-во уникальных пар `(session_id, product_id)` из данной `category_id` с событием `PRODUCT_IMPRESSION`;
- `n_clicks` - кол-во уникальных пар `(session_id, product_id)` из данной `category_id` с событием `PRODUCT_VIEW`;
- `n_atcs` - кол-во уникальных пар `(session_id, product_id)` из данной `category_id` с событием `ADD_TO_CART`;
- `n_atfs` - кол-во уникальных пар `(session_id, product_id)` из данной `category_id` с событием `ADD_TO_FAVORITES`.

## Оркестрация

DAG `feature-platform.layers.silver.account_id_category_id.account_l2_event_w_imps_counts` запускается ежедневно в 00:00 по `Asia/Tashkent` (`schedule_interval="0 19 * * *"` в UTC). Таблица получает стандартные сгенерированные dbt DQ-тесты по primary key после синка источников.
