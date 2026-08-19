"""ClickHouse query for the account lifetime facts snapshot.

Источник — `marketing.account_properties`, строка на `account_id`. Адаптация
`model/features/sql/account_lifetime_facts.sql` (MAD-13227) под ежедневный
снапшот фича-платформы: без хэш-сэмпла.

Берём только поля «первого» и регистрационного ряда: они уже произошли и не
меняются. Поля текущего состояния (segment, RFM, last_*, is_banned) НЕ берём —
их сегодняшнее значение было бы будущим относительно исторического решения
(прецедент утечки MAD-13227: шесть колонок, ROC-AUC 0.7702 -> 0.7540).

Популяция: аккаунты с хотя бы одним созданным заказом и активностью за
последние 200 дней (date - 200). Фильтр по `last_order_date_created_uz` —
только отбор популяции, не признак: покрывает всех, кто может попасть в
дневную популяцию gold-витрины признаков (окно истории 182 дня + запас).
Замер 2026-08-12: 4.36 млн строк из 8.86 млн аккаунтов с заказами.

Оговорки прототипа сохраняются: `fo_date_issued_uz` — про первый ВЫКУПЛЕННЫЙ
заказ, при сборке обучающего набора зануляется, если позже даты решения;
`cnt_accounts_per_installs` — текущее значение, истории нет, признак
приблизительный.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Tuple

SOURCE_TABLE = "marketing.account_properties"

# Окно активности для отбора популяции, дней.
ACTIVITY_WINDOW_DAYS = 200

SQL = f"""
SELECT
    account_id,
    fo_date_created_uz                              AS first_order_date_ever,
    fo_id_created                                   AS first_order_id_ever,
    fo_date_issued_uz                               AS first_issued_order_date,
    fo_issued_payment_type                          AS first_issued_payment_type,
    fo_issued_paymart_type                          AS first_issued_paymart_type,
    registration_date_uz                            AS registration_date,
    first_session_started_date_uz                   AS first_session_date,
    first_city_id_created                           AS first_city_id,
    first_delivery_point_type_created               AS first_delivery_point_type,
    source_type                                     AS acquisition_source_type,
    campaign_type                                   AS acquisition_campaign_type,
    cnt_accounts_per_installs                       AS accounts_per_install_current
FROM {SOURCE_TABLE}
WHERE account_id > 0
  AND fo_date_created_uz > toDate('1970-01-01')
  AND last_order_date_created_uz >= toDate(%(partition_date)s) - {ACTIVITY_WINDOW_DAYS}
"""


def build_query(partition_date: date) -> Tuple[str, Dict[str, Any]]:
    """SQL и параметры снапшота на дату партиции."""
    return SQL, {"partition_date": partition_date.isoformat()}
