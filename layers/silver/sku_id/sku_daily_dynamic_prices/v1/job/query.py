"""ClickHouse query for the daily SKU dynamic-pricing price aggregate.

`pricing.dynamic_discount` содержит только дино-скидки; скидки по карте живут в
отдельных плечах Trino-источника и в итоговую цену здесь не входят.

Агрегация идет по всем `promotion_id` за сутки. Плечи — это A/B-варианты дино-модели,
и каждый SKU присутствует минимум в двух, поэтому мода считается по наблюдениям
«плечо x батч» и не взвешена трафиком.

Мода считается точно, двухуровневой агрегацией: сначала счетчики по цене, затем
`argMax` по паре (частота, последний батч). Приближенный `topK` не используется:
на SKU с двумя близкими по частоте ценами он дает недетерминированный результат.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Tuple

SOURCE_TABLE = "pricing.dynamic_discount"

# created_at имеет тип DateTime('UTC'), а серверная таймзона ClickHouse —
# Asia/Tashkent, поэтому зона у литерала окна указана явно: без нее границы
# суток уезжают на 5 часов.
SQL = f"""
WITH observations AS (
    SELECT
        sku_id,
        calculated_for_price AS seller_price,
        calculated_for_price - discount_amount AS dp_sell_price,
        created_at
    FROM {SOURCE_TABLE}
    WHERE created_at >= toDateTime(%(window_start)s, 'UTC')
      AND created_at < toDateTime(%(window_end)s, 'UTC')
),
dp_counts AS (
    SELECT sku_id, dp_sell_price, count() AS cnt, max(created_at) AS last_seen
    FROM observations
    GROUP BY sku_id, dp_sell_price
),
seller_counts AS (
    SELECT sku_id, seller_price, count() AS cnt, max(created_at) AS last_seen
    FROM observations
    GROUP BY sku_id, seller_price
),
dp AS (
    SELECT
        sku_id,
        argMax(dp_sell_price, (cnt, last_seen)) AS dp_sell_price,
        arraySort(groupUniqArray(dp_sell_price)) AS prices,
        toInt32(sum(cnt)) AS observations
    FROM dp_counts
    GROUP BY sku_id
),
seller AS (
    SELECT
        sku_id,
        argMax(seller_price, (cnt, last_seen)) AS seller_price
    FROM seller_counts
    GROUP BY sku_id
)
SELECT
    toDate(%(partition_date)s) AS date,
    dp.sku_id AS sku_id,
    seller.seller_price AS seller_price,
    dp.dp_sell_price AS dp_sell_price,
    dp.prices AS prices,
    dp.observations AS observations
FROM dp
INNER JOIN seller ON seller.sku_id = dp.sku_id
"""


def build_query(partition_date: date) -> Tuple[str, Dict[str, Any]]:
    """SQL и параметры агрегата за закрытые UTC-сутки `partition_date`."""
    window_start = datetime.combine(partition_date, time.min)
    window_end = window_start + timedelta(days=1)
    return SQL, {
        "partition_date": partition_date.isoformat(),
        "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
        "window_end": window_end.strftime("%Y-%m-%d %H:%M:%S"),
    }
