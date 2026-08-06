"""Trino query for the daily account demographics snapshot."""

from __future__ import annotations

from datetime import date


def _date_literal(value: date | str) -> str:
    parsed = date.fromisoformat(str(value))
    return f"DATE '{parsed.isoformat()}'"


def build_query(
    snapshot_date: date,
    customer_table: str,
    ecosystem_users_table: str,
    birth_date_placeholder: date | str,
) -> str:
    snapshot_date_sql = _date_literal(snapshot_date)
    birth_date_placeholder_sql = _date_literal(birth_date_placeholder)

    return f"""
WITH um AS (
    SELECT
        CAST(account_id AS BIGINT) AS account_id,
        CASE UPPER(TRIM(CAST(sex AS VARCHAR)))
            WHEN 'MAN' THEN 'M'
            WHEN 'WOMAN' THEN 'F'
        END AS gender
    FROM {customer_table}
),
ecosystem AS (
    SELECT
        CAST(last_user_id_m AS BIGINT) AS account_id,
        CASE UPPER(TRIM(CAST(last_gender_ub AS VARCHAR)))
            WHEN 'M' THEN 'M'
            WHEN 'F' THEN 'F'
        END AS gender,
        NULLIF(
            TRY_CAST(birth_year_UB AS DATE),
            {birth_date_placeholder_sql}
        ) AS birth_date
    FROM {ecosystem_users_table}
),
joined AS (
    SELECT
        COALESCE(um.account_id, ecosystem.account_id) AS account_id,
        COALESCE(um.gender, ecosystem.gender) AS gender,
        ecosystem.birth_date
    FROM um
    FULL OUTER JOIN ecosystem
        ON um.account_id = ecosystem.account_id
)
SELECT
    {snapshot_date_sql} AS snapshot_date,
    account_id,
    gender,
    CAST(
        CASE
            WHEN birth_date IS NULL
              OR birth_date > {snapshot_date_sql}
            THEN NULL
            ELSE
                YEAR({snapshot_date_sql}) - YEAR(birth_date)
                - CASE
                    WHEN MONTH({snapshot_date_sql}) < MONTH(birth_date)
                      OR (
                          MONTH({snapshot_date_sql}) = MONTH(birth_date)
                          AND DAY({snapshot_date_sql}) < DAY(birth_date)
                      )
                    THEN 1
                    ELSE 0
                  END
        END AS INTEGER
    ) AS age
FROM joined
"""
