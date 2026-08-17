"""Собрать дневной as-of срез признаков истории выкупа по аккаунту."""

from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from job.entities import Arguments
from job.partition import parse_partition_date


# As-of состояние позиций заказа на конец суток analyze_date.
HISTORY_ORDER_ITEMS_TABLE = "iceberg.silver.history_order_items"
# Сырые позиции заказов: нужны для счётчиков создания на горизонте 365 дней,
# который не помещается в окно среза (182 дня).
ORDER_ITEMS_TABLE = "iceberg.silver.order_items"
# Пожизненные факты аккаунта: настоящий стаж, первый заказ, регистрация, привлечение.
ACCOUNT_LIFETIME_FACTS_TABLE = "iceberg.silver.feature_platform_account_lifetime_facts"

BUSINESS_TIMEZONE = "Asia/Tashkent"
# Горизонт счётчиков создания заказов.
CREATION_WINDOW_DAYS = 365
# Запас сканирования по purchased_at: оплата может отстоять от generated_at.
CREATION_SCAN_BUFFER_DAYS = 7
# Окно среза витрины 182 дня приближается как «минус полгода»; расхождение до 4 дней
# гасится семидневным буфером признака history_left_censored.
HISTORY_LEFT_CENSOR_BUFFER_DAYS = 7


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def asof_history_sql(run_date: date) -> str:
    """Признаки истории выкупа по срезу analyze_date = D (порт user_features_asof.sql).

    Точка отсчёта — конец суток D по Ташкенту: срез обрывается ровно там, поэтому
    заглянуть в будущее из него нельзя.

    Два осознанных пропуска прототипа сохранены:
      * денежные ставки считаются через gmv_completed, а не gmv_net: объёмы возвратов
        в витрине берутся из текущего состояния и в старых срезах не as-of;
      * стаж и первый заказ «за всю жизнь» здесь не считаются — окно среза 182 дня,
        они приходят из silver-витрины пожизненных фактов.
    """
    snapshot_date = run_date.isoformat()

    return f"""
WITH hist AS (
    SELECT
        h.account_id,
        h.order_id,
        h.order_item_id,
        h.generated_at,
        h.delivered_at,
        h.issued_at,
        h.returned_at,
        h.delivery_type,
        h.payment_type,
        h.order_city_id,
        h.order_region_id,
        h.delivery_point_id,
        h.item_quantity,
        h.real_order_item_status                     AS st,
        NULLIF(h.return_cause, '')                   AS return_cause,
        h.gmv_generated,
        h.gmv_completed
    FROM {HISTORY_ORDER_ITEMS_TABLE} AS h
    WHERE h.analyze_date = DATE('{snapshot_date}')
      AND h.account_id > 0
),

item_flags AS (
    SELECT
        t.*,
        -- у курьерки delivered_at всегда пуст — доставку фиксируем по вручению
        CASE WHEN t.delivered_at IS NOT NULL
                  OR (t.delivery_type = 'COURIER' AND t.issued_at IS NOT NULL)
             THEN 1 ELSE 0 END AS is_delivered,
        CASE WHEN t.st = 'ACTIVE'    THEN 1 ELSE 0 END AS is_active,
        CASE WHEN t.st = 'COMPLETED' THEN 1 ELSE 0 END AS is_completed,
        CASE WHEN t.st = 'RETURNED BEFORE DELIVERY' THEN 1 ELSE 0 END AS is_cbd,
        CASE WHEN t.return_cause IN ('MISSING', 'DEFECTED', 'BAD_QUALITY',
                                     'WRONG_ITEM', 'PHOTO_MISMATCH', 'CONTENT')
                  AND t.returned_at IS NOT NULL
             THEN 1 ELSE 0 END AS is_fair_return,
        -- четыре составляющие невыкупа (как в MAD-11218)
        CASE WHEN t.issued_at IS NULL AND t.returned_at IS NOT NULL AND t.delivered_at IS NOT NULL
                  AND TIMESTAMPDIFF(DAY, t.delivered_at, t.returned_at) >= 6
                  AND t.return_cause = 'CANCELED'
             THEN 1 ELSE 0 END AS c_no_show,
        CASE WHEN t.issued_at IS NULL AND t.returned_at IS NOT NULL AND t.delivered_at IS NOT NULL
                  AND TIMESTAMPDIFF(DAY, t.delivered_at, t.returned_at) < 6
                  AND t.return_cause = 'CANCELED'
             THEN 1 ELSE 0 END AS c_cancel_after_delivery,
        CASE WHEN (t.issued_at IS NOT NULL AND t.returned_at IS NOT NULL
                   AND TIMESTAMPDIFF(MINUTE, t.issued_at, t.returned_at) BETWEEN 0 AND 60
                   AND COALESCE(t.return_cause, 'NONE') NOT IN ('CANCELED', 'MISSING'))
               OR (t.issued_at IS NULL AND t.returned_at IS NOT NULL AND t.delivered_at IS NOT NULL
                   AND COALESCE(t.return_cause, 'NONE') NOT IN ('CANCELED'))
             THEN 1 ELSE 0 END AS c_return_at_handover,
        CASE WHEN t.issued_at IS NOT NULL AND t.returned_at IS NOT NULL
                  AND TIMESTAMPDIFF(MINUTE, t.issued_at, t.returned_at) > 60
                  AND COALESCE(t.return_cause, 'NONE') NOT IN ('CANCELED', 'MISSING')
             THEN 1 ELSE 0 END AS c_return_post_handover,
        -- клиентский невыкуп с курьеркой
        CASE WHEN t.st = 'RETURNED NO SHOW'
                  OR (t.st = 'RETURNED' AND t.return_cause = 'CANCELED')
             THEN 1 ELSE 0 END AS is_nonbuyout_client
    FROM hist AS t
),

-- история, свёрнутая до заказа
hist_orders AS (
    SELECT
        f.account_id,
        f.order_id,
        MIN(f.generated_at)                                            AS order_ts,
        TO_DATE(FROM_UTC_TIMESTAMP(MIN(f.generated_at), '{BUSINESS_TIMEZONE}')) AS order_date,
        MIN_BY(f.payment_type,      f.order_item_id)                   AS payment_type,
        MIN_BY(f.delivery_type,     f.order_item_id)                   AS delivery_type,
        MIN_BY(f.order_city_id,     f.order_item_id)                   AS city_id,
        MIN_BY(f.order_region_id,   f.order_item_id)                   AS region_id,
        MIN_BY(f.delivery_point_id, f.order_item_id)                   AS delivery_point_id,
        COUNT(*)                                                       AS n_items,
        SUM(f.item_quantity)                                           AS n_units,
        SUM(f.is_active)                                               AS n_active,
        SUM(f.is_delivered)                                            AS n_delivered,
        SUM(f.is_completed)                                            AS n_completed,
        SUM(f.is_cbd)                                                  AS n_cbd,
        SUM(f.is_nonbuyout_client)                                     AS n_nonbuyout_client,
        SUM(f.c_no_show)                                               AS n_no_show,
        SUM(f.c_cancel_after_delivery)                                 AS n_cancel_after_delivery,
        SUM(f.c_return_at_handover)                                    AS n_return_at_handover,
        SUM(f.c_return_post_handover)                                  AS n_return_post_handover,
        SUM(f.is_fair_return)                                          AS n_fair_return,
        CAST(SUM(f.gmv_generated) AS DOUBLE)                           AS gmv_generated,
        CAST(SUM(CASE WHEN f.is_delivered = 1 THEN f.gmv_generated ELSE 0 END) AS DOUBLE) AS gmv_delivered,
        CAST(SUM(f.gmv_completed) AS DOUBLE)                           AS gmv_completed,
        CAST(SUM(CASE WHEN f.is_fair_return = 1 THEN f.gmv_generated ELSE 0 END) AS DOUBLE) AS gmv_fair_return,
        CAST(SUM(CASE WHEN f.c_no_show = 1 THEN f.gmv_generated ELSE 0 END) AS DOUBLE) AS gmv_no_show,
        -- когда невыкуп стал виден в данных — по времени создания возврата
        MIN(CASE WHEN f.is_nonbuyout_client = 1 THEN f.returned_at END) AS nonbuyout_event_ts,
        MAX_BY(CASE WHEN f.is_nonbuyout_client = 1 THEN
                    CASE WHEN f.c_no_show = 1 THEN 'no_show'
                         WHEN f.c_cancel_after_delivery = 1 THEN 'cancel_after_delivery'
                         ELSE 'courier_or_other' END END,
               COALESCE(f.returned_at, TIMESTAMP '1970-01-01 00:00:00'))  AS last_nonbuyout_type_in_order,
        MAX_BY(CASE WHEN f.is_nonbuyout_client = 1 THEN f.return_cause END,
               COALESCE(f.returned_at, TIMESTAMP '1970-01-01 00:00:00'))  AS last_nonbuyout_cause_in_order
    FROM item_flags AS f
    GROUP BY f.account_id, f.order_id
),

ord AS (
    SELECT
        o.*,
        CASE WHEN o.n_active = 0 THEN 1 ELSE 0 END                        AS is_resolved,
        CASE WHEN o.n_delivered > 0 THEN 1 ELSE 0 END                     AS is_delivered_order,
        CASE WHEN o.n_nonbuyout_client > 0 THEN 1 ELSE 0 END              AS is_bad_order,
        CASE WHEN o.payment_type IN ('PostPaid', 'Postpaid') THEN 1 ELSE 0 END AS is_postpaid,
        CASE WHEN o.payment_type IN ('Nasiya', 'Paymart', 'PaymartMfo', 'PaymartMFO')
             THEN 1 ELSE 0 END                                            AS is_installment,
        -- выкупаемость заказа в деньгах; gmv_net не используем — см. docstring
        CASE WHEN (o.gmv_delivered - o.gmv_fair_return) > 0
             THEN o.gmv_completed / (o.gmv_delivered - o.gmv_fair_return)
             ELSE NULL END                                                AS order_buyout_rate,
        DATEDIFF(DATE('{snapshot_date}'), o.order_date)                   AS days_ago
    FROM hist_orders AS o
),

-- завершённые доставленные заказы по порядку — для трендов и серий
resolved AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (PARTITION BY r.account_id ORDER BY r.order_ts,      r.order_id)      AS seq_asc,
        ROW_NUMBER() OVER (PARTITION BY r.account_id ORDER BY r.order_ts DESC, r.order_id DESC) AS seq_desc,
        COUNT(*)     OVER (PARTITION BY r.account_id)                                           AS n_resolved
    FROM ord AS r
    WHERE r.is_resolved = 1 AND r.is_delivered_order = 1
),

-- текущая серия одинаковых исходов
streak_src AS (
    SELECT
        account_id,
        -- Spark не умеет ARRAY_AGG(... ORDER BY ...): собираем структуру и сортируем
        TRANSFORM(
            SORT_ARRAY(COLLECT_LIST(STRUCT(seq_desc AS ord, is_bad_order AS is_bad))),
            item -> item.is_bad
        ) AS bad_seq
    FROM resolved
    GROUP BY account_id
),
streaks AS (
    SELECT
        account_id,
        ELEMENT_AT(bad_seq, 1) AS last_outcome_is_bad,
        AGGREGATE(
            bad_seq,
            STRUCT(CAST(0 AS BIGINT) AS cnt, TRUE AS still),
            (s, x) -> IF(s.still AND x = ELEMENT_AT(bad_seq, 1),
                         STRUCT(s.cnt + 1 AS cnt, TRUE  AS still),
                         STRUCT(s.cnt     AS cnt, FALSE AS still)),
            s -> s.cnt
        ) AS current_streak_len
    FROM streak_src
),

-- агрегаты по пользователю
agg AS (
    SELECT
        o.account_id,

        -- объём истории (в пределах окна среза 182 дня)
        COUNT(*)                                                          AS n_orders_win,
        SUM(o.is_delivered_order)                                         AS n_delivered_orders_win,
        SUM(CASE WHEN o.is_resolved = 1 AND o.is_delivered_order = 1 THEN 1 ELSE 0 END) AS n_resolved_orders_win,
        SUM(CASE WHEN o.days_ago <= 30 THEN 1 ELSE 0 END)                 AS n_orders_30d,
        SUM(CASE WHEN o.days_ago <= 90 THEN 1 ELSE 0 END)                 AS n_orders_90d,
        SUM(CASE WHEN o.n_active > 0 THEN 1 ELSE 0 END)                   AS n_orders_in_processing,

        -- выкупаемость в деньгах (gross, fair-знаменатель)
        SUM(o.gmv_completed)                                              AS gmv_completed_win,
        SUM(o.gmv_delivered)                                              AS gmv_delivered_win,
        SUM(o.gmv_fair_return)                                            AS gmv_fair_return_win,
        SUM(CASE WHEN o.days_ago <= 90 THEN o.gmv_completed ELSE 0 END)   AS gmv_completed_90d,
        SUM(CASE WHEN o.days_ago <= 90 THEN o.gmv_delivered ELSE 0 END)   AS gmv_delivered_90d,
        SUM(CASE WHEN o.days_ago <= 90 THEN o.gmv_fair_return ELSE 0 END) AS gmv_fair_return_90d,
        SUM(CASE WHEN o.days_ago <= 30 THEN o.gmv_completed ELSE 0 END)   AS gmv_completed_30d,
        SUM(CASE WHEN o.days_ago <= 30 THEN o.gmv_delivered ELSE 0 END)   AS gmv_delivered_30d,
        SUM(CASE WHEN o.days_ago <= 30 THEN o.gmv_fair_return ELSE 0 END) AS gmv_fair_return_30d,

        -- выкупаемость в штуках позиций
        SUM(o.n_completed)                                                AS n_items_completed_win,
        SUM(o.n_delivered)                                                AS n_items_delivered_win,

        -- состав невыкупа
        SUM(o.n_no_show)                                                  AS n_items_no_show,
        SUM(o.n_cancel_after_delivery)                                    AS n_items_cancel_after_delivery,
        SUM(o.n_return_at_handover)                                       AS n_items_return_at_handover,
        SUM(o.n_return_post_handover)                                     AS n_items_return_post_handover,
        SUM(o.n_fair_return)                                              AS n_items_fair_return,
        SUM(o.n_cbd)                                                      AS n_items_cancel_before_delivery,
        SUM(CASE WHEN o.n_cbd = o.n_items THEN 1 ELSE 0 END)              AS n_orders_cancelled_before_delivery,
        SUM(o.gmv_no_show)                                                AS gmv_no_show_win,

        -- события клиентского невыкупа
        SUM(o.is_bad_order)                                               AS n_nonbuyout_events,
        MIN(o.nonbuyout_event_ts)                                         AS first_nonbuyout_event_ts,
        MAX(o.nonbuyout_event_ts)                                         AS last_nonbuyout_event_ts,
        MAX_BY(o.last_nonbuyout_type_in_order,
               COALESCE(o.nonbuyout_event_ts, TIMESTAMP '1970-01-01 00:00:00'))  AS last_nonbuyout_type,
        MAX_BY(o.last_nonbuyout_cause_in_order,
               COALESCE(o.nonbuyout_event_ts, TIMESTAMP '1970-01-01 00:00:00'))  AS last_nonbuyout_cause,

        -- способ оплаты в истории
        SUM(o.is_postpaid)                                                AS n_orders_postpaid,
        SUM(o.is_installment)                                             AS n_orders_installment,
        SUM(CASE WHEN o.is_postpaid = 1 THEN o.gmv_completed ELSE 0 END)  AS gmv_completed_postpaid,
        SUM(CASE WHEN o.is_postpaid = 1 THEN o.gmv_delivered - o.gmv_fair_return ELSE 0 END) AS gmv_denom_postpaid,
        SUM(CASE WHEN o.is_postpaid = 0 AND o.is_installment = 0 THEN o.gmv_completed ELSE 0 END) AS gmv_completed_prepaid,
        SUM(CASE WHEN o.is_postpaid = 0 AND o.is_installment = 0 THEN o.gmv_delivered - o.gmv_fair_return ELSE 0 END) AS gmv_denom_prepaid,
        MIN_BY(o.is_postpaid, o.order_ts)                                 AS first_order_in_win_is_postpaid,
        -- последний способ оплаты в истории
        MAX_BY(o.payment_type, o.order_ts)                                AS last_order_payment_type,
        MAX_BY(o.is_postpaid,  o.order_ts)                                AS last_order_is_postpaid,

        -- чек и состав корзины в истории
        AVG(o.gmv_generated)                                              AS avg_ticket_win,
        -- Отрицание воспроизводит конвенцию Trino APPROX_PERCENTILE (верхняя
        -- медиана при чётном числе заказов): Spark PERCENTILE_APPROX берёт
        -- нижнюю, и признак систематически расходился бы с обучающим набором
        -- MAD-13227 у 38% аккаунтов (паритетный смоук 12.08.2026).
        -PERCENTILE_APPROX(-o.gmv_generated, 0.5)                         AS median_ticket_win,
        STDDEV_POP(o.gmv_generated)                                       AS std_ticket_win,
        MAX(o.gmv_generated)                                              AS max_ticket_win,
        AVG(CAST(o.n_items AS DOUBLE))                                    AS avg_items_per_order_win,

        -- гео и ПВЗ истории; полноценные гео-признаки — отдельная задача (MAD-13228)
        COUNT(DISTINCT o.delivery_point_id)                               AS n_distinct_dp_win,
        COUNT(DISTINCT o.city_id)                                         AS n_distinct_city_win,
        -- ключи связи с гео-витринами сервиса (order_completion_city/region_features
        -- присоединяются по date + 1): город и регион последнего заказа окна
        MAX_BY(o.city_id,   o.order_ts)                                   AS last_order_city_id,
        MAX_BY(o.region_id, o.order_ts)                                   AS last_order_region_id,

        -- временные метки
        MIN(o.order_date)                                                 AS first_order_date_win,
        MAX(o.order_date)                                                 AS last_order_date_win
    FROM ord AS o
    GROUP BY o.account_id
),

-- последние N завершённых заказов
lastn AS (
    SELECT
        account_id,
        AVG(CASE WHEN seq_desc <= 3  THEN order_buyout_rate END) AS buyout_rate_last_3,
        AVG(CASE WHEN seq_desc <= 5  THEN order_buyout_rate END) AS buyout_rate_last_5,
        AVG(CASE WHEN seq_desc <= 10 THEN order_buyout_rate END) AS buyout_rate_last_10,
        AVG(CASE WHEN seq_asc  <= 3  THEN order_buyout_rate END) AS buyout_rate_first_3,
        MAX(CASE WHEN seq_desc = 1 THEN is_bad_order END)        AS prev_order_is_nonbuyout,
        MAX(CASE WHEN seq_desc = 2 THEN is_bad_order END)        AS prev2_order_is_nonbuyout,
        MAX(CASE WHEN seq_desc = 3 THEN is_bad_order END)        AS prev3_order_is_nonbuyout,
        SUM(CASE WHEN seq_desc <= 3  THEN is_bad_order ELSE 0 END) AS n_bad_last_3,
        SUM(CASE WHEN seq_desc <= 5  THEN is_bad_order ELSE 0 END) AS n_bad_last_5,
        SUM(CASE WHEN seq_desc <= 10 THEN is_bad_order ELSE 0 END) AS n_bad_last_10,
        MAX(n_resolved)                                          AS n_resolved_seq
    FROM resolved
    GROUP BY account_id
)

SELECT
    a.account_id,

    -- объём истории
    a.n_orders_win, a.n_delivered_orders_win, a.n_resolved_orders_win,
    a.n_orders_30d, a.n_orders_90d, a.n_orders_in_processing,

    -- выкупаемость: в деньгах, в позициях и в заказах
    CASE WHEN (a.gmv_delivered_win - a.gmv_fair_return_win) > 0
         THEN a.gmv_completed_win / (a.gmv_delivered_win - a.gmv_fair_return_win) END AS buyout_rate_money_win,
    CASE WHEN (a.gmv_delivered_90d - a.gmv_fair_return_90d) > 0
         THEN a.gmv_completed_90d / (a.gmv_delivered_90d - a.gmv_fair_return_90d) END AS buyout_rate_money_90d,
    CASE WHEN (a.gmv_delivered_30d - a.gmv_fair_return_30d) > 0
         THEN a.gmv_completed_30d / (a.gmv_delivered_30d - a.gmv_fair_return_30d) END AS buyout_rate_money_30d,
    CASE WHEN a.n_items_delivered_win > 0
         THEN CAST(a.n_items_completed_win AS DOUBLE) / a.n_items_delivered_win END   AS buyout_rate_items_win,
    CASE WHEN a.n_delivered_orders_win > 0
         THEN 1.0 - CAST(a.n_nonbuyout_events AS DOUBLE) / a.n_delivered_orders_win END AS buyout_rate_orders_win,

    l.buyout_rate_last_3, l.buyout_rate_last_5, l.buyout_rate_last_10, l.buyout_rate_first_3,
    -- тренд выкупа: последние три заказа против первых трёх (как в MAD-11574).
    -- При меньше чем четырёх завершённых оба окна — одни и те же заказы и тренд
    -- всегда ноль, поэтому отдаём NULL: «мало истории» не должно выглядеть
    -- как «динамики нет»
    CASE WHEN l.n_resolved_seq >= 4
         THEN l.buyout_rate_last_3 - l.buyout_rate_first_3 END                         AS buyout_trend,

    -- серии
    COALESCE(l.prev_order_is_nonbuyout, 0)   AS prev_order_is_nonbuyout,
    COALESCE(l.prev2_order_is_nonbuyout, 0)  AS prev2_order_is_nonbuyout,
    COALESCE(l.prev3_order_is_nonbuyout, 0)  AS prev3_order_is_nonbuyout,
    l.n_bad_last_3, l.n_bad_last_5, l.n_bad_last_10,
    CASE WHEN s.last_outcome_is_bad = 0 THEN s.current_streak_len ELSE 0 END AS buyout_streak,
    CASE WHEN s.last_outcome_is_bad = 1 THEN s.current_streak_len ELSE 0 END AS nonbuyout_streak,

    -- состав невыкупа
    a.n_items_no_show, a.n_items_cancel_after_delivery,
    a.n_items_return_at_handover, a.n_items_return_post_handover,
    a.n_items_fair_return, a.n_items_cancel_before_delivery,
    a.n_orders_cancelled_before_delivery,
    CASE WHEN a.n_items_delivered_win > 0
         THEN CAST(a.n_items_no_show AS DOUBLE) / a.n_items_delivered_win END          AS no_show_share_of_delivered,
    CASE WHEN (a.n_items_no_show + a.n_items_cancel_after_delivery
               + a.n_items_return_at_handover + a.n_items_return_post_handover) > 0
         THEN CAST(a.n_items_no_show AS DOUBLE)
              / (a.n_items_no_show + a.n_items_cancel_after_delivery
                 + a.n_items_return_at_handover + a.n_items_return_post_handover) END  AS no_show_share_of_nonbuyout,
    CASE WHEN a.n_orders_win > 0
         THEN CAST(a.n_orders_cancelled_before_delivery AS DOUBLE) / a.n_orders_win END AS cancel_before_delivery_share,
    CASE WHEN a.gmv_delivered_win > 0
         THEN a.gmv_no_show_win / a.gmv_delivered_win END                              AS no_show_gmv_share,

    -- первый и последний невыкуп
    a.n_nonbuyout_events,
    CASE WHEN a.n_nonbuyout_events > 0 THEN 1 ELSE 0 END                               AS is_after_first_non_buyout,
    CASE WHEN a.first_nonbuyout_event_ts IS NOT NULL
         THEN DATEDIFF(DATE('{snapshot_date}'),
                       TO_DATE(FROM_UTC_TIMESTAMP(a.first_nonbuyout_event_ts, '{BUSINESS_TIMEZONE}'))) END AS days_since_first_nonbuyout,
    CASE WHEN a.last_nonbuyout_event_ts IS NOT NULL
         THEN DATEDIFF(DATE('{snapshot_date}'),
                       TO_DATE(FROM_UTC_TIMESTAMP(a.last_nonbuyout_event_ts, '{BUSINESS_TIMEZONE}'))) END  AS days_since_last_nonbuyout,
    a.last_nonbuyout_type,
    a.last_nonbuyout_cause,

    -- способ оплаты в истории
    CASE WHEN a.n_orders_win > 0
         THEN CAST(a.n_orders_postpaid AS DOUBLE) / a.n_orders_win END                 AS postpaid_share_win,
    CASE WHEN a.n_orders_win > 0
         THEN CAST(a.n_orders_installment AS DOUBLE) / a.n_orders_win END              AS installment_share_win,
    CASE WHEN a.gmv_denom_postpaid > 0
         THEN a.gmv_completed_postpaid / a.gmv_denom_postpaid END                      AS buyout_rate_postpaid,
    CASE WHEN a.gmv_denom_prepaid > 0
         THEN a.gmv_completed_prepaid / a.gmv_denom_prepaid END                        AS buyout_rate_prepaid,
    a.first_order_in_win_is_postpaid,
    a.last_order_payment_type,
    a.last_order_is_postpaid,

    -- чек и корзина в истории
    a.avg_ticket_win, a.median_ticket_win, a.std_ticket_win, a.max_ticket_win,
    a.avg_items_per_order_win,
    a.n_distinct_dp_win, a.n_distinct_city_win,

    -- временные метки и цензура окна
    a.first_order_date_win,
    a.last_order_date_win,
    DATEDIFF(DATE('{snapshot_date}'), a.last_order_date_win)                           AS days_since_last_order_win,
    DATEDIFF(DATE('{snapshot_date}'), a.first_order_date_win)                          AS tenure_days_win,
    -- окно среза 182 дня: если самый ранний заказ пользователя лежит у левого края,
    -- его настоящая история заведомо длиннее и признаки «первого заказа» занижены
    CASE WHEN a.first_order_date_win
              <= DATE_ADD(ADD_MONTHS(DATE_ADD(DATE('{snapshot_date}'), 1), -6), {HISTORY_LEFT_CENSOR_BUFFER_DAYS})
         THEN 1 ELSE 0 END                                                             AS history_left_censored
FROM agg AS a
LEFT JOIN lastn   AS l ON l.account_id = a.account_id
LEFT JOIN streaks AS s ON s.account_id = a.account_id
"""


def orders_created_sql(run_date: date) -> str:
    """Счётчики созданных заказов на конец суток D (порт user_creation_facts.sql).

    Утечки нет: читается только момент создания заказа. Признак «заказы того же дня
    до текущего» в витрину не входит — на конец суток D его считать не от чего,
    это обязанность онлайн-сервиса на момент решения.
    """
    scan_start = run_date - timedelta(days=CREATION_WINDOW_DAYS + CREATION_SCAN_BUFFER_DAYS)
    # Верхняя граница окна счётчиков — полночь Ташкента дня D + 1, то есть конец суток D.
    cutoff = run_date + timedelta(days=1)
    # Запас справа: purchased_at может отставать от generated_at.
    scan_end = cutoff + timedelta(days=2)
    cutoff_expr = f"TO_UTC_TIMESTAMP(TIMESTAMP '{cutoff.isoformat()} 00:00:00', '{BUSINESS_TIMEZONE}')"

    return f"""
WITH orders_raw AS (
    SELECT
        oi.account_id,
        oi.order_id,
        MIN(oi.generated_at) AS generated_at
    FROM {ORDER_ITEMS_TABLE} AS oi
    WHERE oi.purchased_at >= TO_UTC_TIMESTAMP(TIMESTAMP '{scan_start.isoformat()} 00:00:00', '{BUSINESS_TIMEZONE}')
      AND oi.purchased_at <  TO_UTC_TIMESTAMP(TIMESTAMP '{scan_end.isoformat()} 00:00:00', '{BUSINESS_TIMEZONE}')
      AND oi.account_id > 0
      AND oi.order_item_status NOT IN ('CREATED', 'NOT_CREATED')
    GROUP BY oi.account_id, oi.order_id
),

before_cutoff AS (
    SELECT
        account_id,
        order_id,
        -- расстояние до конца суток D; окна прототипа заданы в сутках по 24 часа,
        -- поэтому считаем в секундах, а не в календарных днях
        TIMESTAMPDIFF(SECOND, generated_at, {cutoff_expr}) AS seconds_before_cutoff
    FROM orders_raw
    WHERE generated_at < {cutoff_expr}
)

SELECT
    account_id,
    COUNT(*) FILTER (WHERE seconds_before_cutoff <=   1 * 86400) AS orders_created_prev_1d,
    COUNT(*) FILTER (WHERE seconds_before_cutoff <=   7 * 86400) AS orders_created_prev_7d,
    COUNT(*) FILTER (WHERE seconds_before_cutoff <=  30 * 86400) AS orders_created_prev_30d,
    COUNT(*) FILTER (WHERE seconds_before_cutoff <=  90 * 86400) AS orders_created_prev_90d,
    COUNT(*) FILTER (WHERE seconds_before_cutoff <= {CREATION_WINDOW_DAYS} * 86400) AS orders_created_prev_365d
FROM before_cutoff
GROUP BY account_id
"""


def lifetime_facts_sql(run_date: date) -> str:
    """Пожизненные факты аккаунта из silver-витрины за партицию D.

    Партиция D — снимок источника внутри суток D; партиция D+1 снята уже после конца
    окна и в as-of витрину не берётся.
    """
    return f"""
SELECT
    account_id,
    first_order_date_ever,
    first_order_id_ever,
    first_issued_order_date,
    first_issued_payment_type,
    first_issued_paymart_type,
    registration_date,
    first_session_date,
    first_city_id,
    first_delivery_point_type,
    acquisition_source_type,
    acquisition_campaign_type,
    accounts_per_install_current
FROM {ACCOUNT_LIFETIME_FACTS_TABLE}
WHERE date = DATE('{run_date.isoformat()}')
"""


def features_sql(run_date: date) -> str:
    """Финальная сборка: as-of история + счётчики создания + пожизненные факты.

    Поля first_issued_* отдаются как есть: обнуление «первого выкупа», который лежит
    в будущем относительно исторической даты решения, делает сборщик обучающего
    набора — витрина описывает состояние на конец суток D.
    """
    snapshot_date = run_date.isoformat()

    return f"""
SELECT
    DATE('{snapshot_date}')                            AS date,
    CAST(h.account_id AS BIGINT)                       AS account_id,

    CAST(h.n_orders_win AS BIGINT)                     AS n_orders_win,
    CAST(h.n_delivered_orders_win AS BIGINT)           AS n_delivered_orders_win,
    CAST(h.n_resolved_orders_win AS BIGINT)            AS n_resolved_orders_win,
    CAST(h.n_orders_30d AS BIGINT)                     AS n_orders_30d,
    CAST(h.n_orders_90d AS BIGINT)                     AS n_orders_90d,
    CAST(h.n_orders_in_processing AS BIGINT)           AS n_orders_in_processing,

    CAST(h.buyout_rate_money_win AS DOUBLE)            AS buyout_rate_money_win,
    CAST(h.buyout_rate_money_90d AS DOUBLE)            AS buyout_rate_money_90d,
    CAST(h.buyout_rate_money_30d AS DOUBLE)            AS buyout_rate_money_30d,
    CAST(h.buyout_rate_items_win AS DOUBLE)            AS buyout_rate_items_win,
    CAST(h.buyout_rate_orders_win AS DOUBLE)           AS buyout_rate_orders_win,
    CAST(h.buyout_rate_last_3 AS DOUBLE)               AS buyout_rate_last_3,
    CAST(h.buyout_rate_last_5 AS DOUBLE)               AS buyout_rate_last_5,
    CAST(h.buyout_rate_last_10 AS DOUBLE)              AS buyout_rate_last_10,
    CAST(h.buyout_rate_first_3 AS DOUBLE)              AS buyout_rate_first_3,
    CAST(h.buyout_trend AS DOUBLE)                     AS buyout_trend,

    CAST(h.prev_order_is_nonbuyout AS INT)             AS prev_order_is_nonbuyout,
    CAST(h.prev2_order_is_nonbuyout AS INT)            AS prev2_order_is_nonbuyout,
    CAST(h.prev3_order_is_nonbuyout AS INT)            AS prev3_order_is_nonbuyout,
    CAST(h.n_bad_last_3 AS BIGINT)                     AS n_bad_last_3,
    CAST(h.n_bad_last_5 AS BIGINT)                     AS n_bad_last_5,
    CAST(h.n_bad_last_10 AS BIGINT)                    AS n_bad_last_10,
    CAST(h.buyout_streak AS BIGINT)                    AS buyout_streak,
    CAST(h.nonbuyout_streak AS BIGINT)                 AS nonbuyout_streak,

    CAST(h.n_items_no_show AS BIGINT)                  AS n_items_no_show,
    CAST(h.n_items_cancel_after_delivery AS BIGINT)    AS n_items_cancel_after_delivery,
    CAST(h.n_items_return_at_handover AS BIGINT)       AS n_items_return_at_handover,
    CAST(h.n_items_return_post_handover AS BIGINT)     AS n_items_return_post_handover,
    CAST(h.n_items_fair_return AS BIGINT)              AS n_items_fair_return,
    CAST(h.n_items_cancel_before_delivery AS BIGINT)   AS n_items_cancel_before_delivery,
    CAST(h.n_orders_cancelled_before_delivery AS BIGINT) AS n_orders_cancelled_before_delivery,
    CAST(h.no_show_share_of_delivered AS DOUBLE)       AS no_show_share_of_delivered,
    CAST(h.no_show_share_of_nonbuyout AS DOUBLE)       AS no_show_share_of_nonbuyout,
    CAST(h.cancel_before_delivery_share AS DOUBLE)     AS cancel_before_delivery_share,
    CAST(h.no_show_gmv_share AS DOUBLE)                AS no_show_gmv_share,

    CAST(h.n_nonbuyout_events AS BIGINT)               AS n_nonbuyout_events,
    CAST(h.is_after_first_non_buyout AS INT)           AS is_after_first_non_buyout,
    CAST(h.days_since_first_nonbuyout AS INT)          AS days_since_first_nonbuyout,
    CAST(h.days_since_last_nonbuyout AS INT)           AS days_since_last_nonbuyout,
    CAST(h.last_nonbuyout_type AS STRING)              AS last_nonbuyout_type,
    CAST(h.last_nonbuyout_cause AS STRING)             AS last_nonbuyout_cause,

    CAST(h.postpaid_share_win AS DOUBLE)               AS postpaid_share_win,
    CAST(h.installment_share_win AS DOUBLE)            AS installment_share_win,
    CAST(h.buyout_rate_postpaid AS DOUBLE)             AS buyout_rate_postpaid,
    CAST(h.buyout_rate_prepaid AS DOUBLE)              AS buyout_rate_prepaid,
    CAST(h.first_order_in_win_is_postpaid AS INT)      AS first_order_in_win_is_postpaid,
    CAST(h.last_order_payment_type AS STRING)          AS last_order_payment_type,
    CAST(h.last_order_is_postpaid AS INT)              AS last_order_is_postpaid,

    CAST(h.avg_ticket_win AS DOUBLE)                   AS avg_ticket_win,
    CAST(h.median_ticket_win AS DOUBLE)                AS median_ticket_win,
    CAST(h.std_ticket_win AS DOUBLE)                   AS std_ticket_win,
    CAST(h.max_ticket_win AS DOUBLE)                   AS max_ticket_win,
    CAST(h.avg_items_per_order_win AS DOUBLE)          AS avg_items_per_order_win,
    CAST(h.n_distinct_dp_win AS BIGINT)                AS n_distinct_dp_win,
    CAST(h.n_distinct_city_win AS BIGINT)              AS n_distinct_city_win,
    CAST(h.last_order_city_id AS BIGINT)               AS last_order_city_id,
    CAST(h.last_order_region_id AS BIGINT)             AS last_order_region_id,

    CAST(h.first_order_date_win AS DATE)               AS first_order_date_win,
    CAST(h.last_order_date_win AS DATE)                AS last_order_date_win,
    CAST(h.days_since_last_order_win AS INT)           AS days_since_last_order_win,
    CAST(h.tenure_days_win AS INT)                     AS tenure_days_win,
    CAST(h.history_left_censored AS INT)               AS history_left_censored,

    CAST(c.orders_created_prev_1d AS BIGINT)           AS orders_created_prev_1d,
    CAST(c.orders_created_prev_7d AS BIGINT)           AS orders_created_prev_7d,
    CAST(c.orders_created_prev_30d AS BIGINT)          AS orders_created_prev_30d,
    CAST(c.orders_created_prev_90d AS BIGINT)          AS orders_created_prev_90d,
    CAST(c.orders_created_prev_365d AS BIGINT)         AS orders_created_prev_365d,

    CAST(f.first_order_date_ever AS DATE)              AS first_order_date_ever,
    CAST(f.first_order_id_ever AS BIGINT)              AS first_order_id_ever,
    CAST(f.first_issued_order_date AS DATE)            AS first_issued_order_date,
    CAST(f.first_issued_payment_type AS STRING)        AS first_issued_payment_type,
    CAST(f.first_issued_paymart_type AS STRING)        AS first_issued_paymart_type,
    CAST(f.registration_date AS DATE)                  AS registration_date,
    CAST(f.first_session_date AS DATE)                 AS first_session_date,
    CAST(f.first_city_id AS STRING)                    AS first_city_id,
    CAST(f.first_delivery_point_type AS STRING)        AS first_delivery_point_type,
    CAST(f.acquisition_source_type AS STRING)          AS acquisition_source_type,
    CAST(f.acquisition_campaign_type AS STRING)        AS acquisition_campaign_type,
    CAST(f.accounts_per_install_current AS INT)        AS accounts_per_install_current,

    -- производные пожизненных фактов: в MAD-13227 они считались на сборке набора
    CAST(DATEDIFF(DATE('{snapshot_date}'), f.first_order_date_ever) AS INT) AS tenure_days_true,
    CAST(DATEDIFF(DATE('{snapshot_date}'), f.registration_date) AS INT)     AS days_since_registration,
    CAST(CASE WHEN f.first_order_date_ever IS NULL THEN NULL
              WHEN f.first_order_date_ever < DATE_SUB(DATE('{snapshot_date}'), 182) THEN 1
              ELSE 0 END AS INT)                                            AS history_left_censored_true
FROM asof_history AS h
LEFT JOIN orders_created AS c ON c.account_id = h.account_id
LEFT JOIN lifetime_facts AS f ON f.account_id = h.account_id
"""


def build_buyout_account_history_features(
    spark: SparkSession,
    run_date: date,
) -> DataFrame:
    spark.sql(asof_history_sql(run_date)).createOrReplaceTempView("asof_history")
    spark.sql(orders_created_sql(run_date)).createOrReplaceTempView("orders_created")
    spark.sql(lifetime_facts_sql(run_date)).createOrReplaceTempView("lifetime_facts")

    return spark.sql(features_sql(run_date))


def save_buyout_account_history_features(
    spark: SparkSession,
    run_date: date,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_buyout_account_history_features(spark, run_date)
    _align_to_target_schema(spark, features, target_table).writeTo(
        target_table
    ).overwritePartitions()


def _align_to_target_schema(
    spark: SparkSession,
    frame: DataFrame,
    target_table: str,
) -> DataFrame:
    target_schema = spark.table(target_table).schema
    aligned_frame = frame

    for field in target_schema.fields:
        if field.name not in aligned_frame.columns:
            aligned_frame = aligned_frame.withColumn(
                field.name,
                F.lit(None).cast(field.dataType),
            )

    return aligned_frame.select(*(F.col(field.name) for field in target_schema.fields))


def run(spark: SparkSession, arguments: Arguments):
    save_buyout_account_history_features(
        spark,
        parse_partition_date(arguments.partition_start),
        arguments.table_name,
    )
