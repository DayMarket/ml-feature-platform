"""Публикация партиции витрины sku_buyout_features в PostgreSQL сервиса невыкупов."""

from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime
from typing import Iterable, Sequence

logger = logging.getLogger("airflow.task")


def batch_to_csv(batch, columns: Sequence[str], updated_at: datetime) -> io.StringIO:
    """CSV одного батча. Пустое поле — NULL: пропуск в признаке нулём не заменяется."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    table = batch.to_pydict()
    row_count = len(table[columns[0]]) if columns else 0
    for index in range(row_count):
        row = []
        for column in columns:
            value = table[column][index]
            row.append("" if value is None else value)
        row.append(updated_at.isoformat(sep=" "))
        writer.writerow(row)
    buffer.seek(0)
    return buffer


def copy_partition(
    cursor,
    batches: Iterable,
    columns: Sequence[str],
    target_table: str,
    updated_at: datetime,
) -> int:
    """TRUNCATE плюс COPY всех батчей. Вызывается внутри открытой транзакции."""
    cursor.execute(f"TRUNCATE TABLE {target_table}")
    column_list = ", ".join(list(columns) + ["updated_at"])
    statement = f"COPY {target_table} ({column_list}) FROM STDIN WITH (FORMAT csv)"
    written = 0
    for batch in batches:
        buffer = batch_to_csv(batch, columns, updated_at)
        cursor.copy_expert(statement, buffer)
        written += batch.num_rows
    return written


def publish(
    iceberg_table,
    partition_date: date,
    columns: Sequence[str],
    connection,
    target_table: str,
    updated_at: datetime,
) -> int:
    """Заменить содержимое целевой таблицы партицией витрины за одну транзакцию."""
    from pyiceberg.expressions import EqualTo

    scan = iceberg_table.scan(
        row_filter=EqualTo("date", partition_date),
        selected_fields=tuple(columns),
    )
    connection.autocommit = False
    try:
        with connection.cursor() as cursor:
            written = copy_partition(
                cursor,
                scan.to_arrow_batch_reader(),
                columns,
                target_table,
                updated_at,
            )
            if written == 0:
                # Пустая партиция затёрла бы рабочую таблицу сервиса.
                raise RuntimeError(
                    f"Partition {partition_date} of {iceberg_table.name()} is empty; "
                    f"refusing to leave {target_table} truncated"
                )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    logger.info("Copied %d rows into %s", written, target_table)
    return written
