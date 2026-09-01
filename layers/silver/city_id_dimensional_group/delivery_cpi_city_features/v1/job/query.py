"""Trino query for the city-level empirical delivery CPI snapshot.

Грейн: date x city_id x dimensional_group. Одна строка = эмпирический CPI
(стоимость логистики на штуку) по заказам города за скользящее окно
`source.lookback_days` дней до даты партиции.

Источники:
  - "dwh-iceberg".silver.preliminary_cm2_by_order_item — фактические аллокации
    затрат прямого и обратного потока (UZS, IFRS), полная история. ДЕДУП
    обязателен (подвох №11 справочника 01_cm2: дубли order_item_id плавают).
  - "dwh-iceberg".silver.order_items — город, количество, флаги потоков.
    Флаги реконструируются из дат (real_order_item_status здесь нет):
      доставлен  = delivered_at IS NOT NULL OR (COURIER и issued_at IS NOT NULL)
      обратный   = returned_at IS NOT NULL И (delivered_at или issued_at не пусты)
                   (отмена до отгрузки обратной логистики не генерирует)
    Сверка с каноническими счётчиками items_* из order_item_ue_buyer на неделе
    01–07.07.2026 (2.0 млн позиций): доставка 98.8% точных совпадений,
    обратный поток 98.3% (недолов 0.12%, ложных 1.45%).
  - "dwh-iceberg".silver.sku — габаритная группа; ''/UNKNOWN дефолтятся к SMALL
    (как в business_parameters_v2.sql).

Взвешивание обратного плеча по типам возврата — по построению: суммируются
фактические затраты всех трёх типов (невыкуп / возврат на выдаче / возврат
после), делятся на суммарные штуки обратного потока.

Фолбэк для тонких городов: в строке продублирован CPI региона и страны по той
же габаритной группе; потребитель выбирает уровень по n_items_*.
"""

from __future__ import annotations

from datetime import date

COSTS_TABLE = '"dwh-iceberg".silver.preliminary_cm2_by_order_item'
ITEMS_TABLE = '"dwh-iceberg".silver.order_items'
SKU_TABLE = '"dwh-iceberg".silver.sku'

# Габаритные группы, для которых в business_parameters_v2 заданы тарифы;
# всё остальное (включая '' и UNKNOWN) считается SMALL.
DIMENSIONAL_GROUPS = ("SMALL", "MEDIUM", "LARGE")


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_query(partition_date: date, lookback_days: int) -> str:
    """SQL дневного снапшота CPI: окно [date - lookback_days, date)."""
    partition_date_sql = _sql_string(partition_date.isoformat())
    dimensional_groups_sql = ", ".join(
        _sql_string(group) for group in DIMENSIONAL_GROUPS
    )

    return f"""
WITH costs AS (
    SELECT
        order_item_id,
        IF(is_nan(forward_flow_execution_cost_uzs), 0.0,
           COALESCE(forward_flow_execution_cost_uzs, 0.0))  AS fwd_uzs,
        IF(is_nan(reverse_flow_execution_cost_uzs), 0.0,
           COALESCE(reverse_flow_execution_cost_uzs, 0.0))  AS rev_uzs
    FROM (
        SELECT
            order_item_id,
            forward_flow_execution_cost_uzs,
            reverse_flow_execution_cost_uzs,
            ROW_NUMBER() OVER (PARTITION BY order_item_id
                               ORDER BY order_issued_at DESC) AS rn
        FROM {COSTS_TABLE}
        WHERE order_created_at >= TIMESTAMP {partition_date_sql} - INTERVAL '{lookback_days}' DAY
          AND order_created_at <  TIMESTAMP {partition_date_sql}
          AND account_id IS NOT NULL AND account_id != 0
    )
    WHERE rn = 1
),

items AS (
    SELECT
        oi.order_item_id,
        oi.city_id,
        oi.region_id,
        CASE WHEN s.dimensional_group IN ({dimensional_groups_sql})
             THEN s.dimensional_group ELSE 'SMALL' END       AS dimensional_group,
        CASE WHEN oi.delivered_at IS NOT NULL
                  OR (oi.delivery_type = 'COURIER' AND oi.issued_at IS NOT NULL)
             THEN oi.item_quantity ELSE 0 END                AS n_delivered,
        CASE WHEN oi.returned_at IS NOT NULL
                  AND (oi.delivered_at IS NOT NULL OR oi.issued_at IS NOT NULL)
             THEN oi.item_quantity ELSE 0 END                AS n_reverse
    FROM {ITEMS_TABLE} AS oi
    LEFT JOIN {SKU_TABLE} AS s ON s.id = oi.sku_id
    WHERE oi.generated_at >= TIMESTAMP {partition_date_sql} - INTERVAL '{lookback_days}' DAY
      AND oi.generated_at <  TIMESTAMP {partition_date_sql}
      AND oi.city_id IS NOT NULL
),

joined AS (
    SELECT i.city_id, i.region_id, i.dimensional_group,
           i.n_delivered, i.n_reverse, c.fwd_uzs, c.rev_uzs
    FROM items i
    JOIN costs c ON c.order_item_id = i.order_item_id
),

by_city AS (
    SELECT
        city_id,
        ARBITRARY(region_id)                                 AS region_id,
        dimensional_group,
        SUM(fwd_uzs)                                         AS fwd_cost_uzs,
        SUM(rev_uzs)                                         AS rev_cost_uzs,
        SUM(n_delivered)                                     AS n_items_delivered,
        SUM(n_reverse)                                       AS n_items_reverse
    FROM joined
    GROUP BY city_id, dimensional_group
)

SELECT
    DATE {partition_date_sql}                                AS date,
    city_id,
    dimensional_group,
    region_id,
    fwd_cost_uzs / NULLIF(n_items_delivered, 0)              AS cpi_forward_uzs,
    rev_cost_uzs / NULLIF(n_items_reverse, 0)                AS cpi_reverse_uzs,
    n_items_delivered,
    n_items_reverse,
    SUM(fwd_cost_uzs) OVER (PARTITION BY region_id, dimensional_group)
        / NULLIF(SUM(n_items_delivered) OVER (PARTITION BY region_id, dimensional_group), 0)
                                                             AS cpi_forward_region_uzs,
    SUM(rev_cost_uzs) OVER (PARTITION BY region_id, dimensional_group)
        / NULLIF(SUM(n_items_reverse) OVER (PARTITION BY region_id, dimensional_group), 0)
                                                             AS cpi_reverse_region_uzs,
    SUM(fwd_cost_uzs) OVER (PARTITION BY dimensional_group)
        / NULLIF(SUM(n_items_delivered) OVER (PARTITION BY dimensional_group), 0)
                                                             AS cpi_forward_country_uzs,
    SUM(rev_cost_uzs) OVER (PARTITION BY dimensional_group)
        / NULLIF(SUM(n_items_reverse) OVER (PARTITION BY dimensional_group), 0)
                                                             AS cpi_reverse_country_uzs
FROM by_city
"""
