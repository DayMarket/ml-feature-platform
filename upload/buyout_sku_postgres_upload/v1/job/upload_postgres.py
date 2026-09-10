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


def stage_table_name(target_table: str) -> str:
    """Имя временной таблицы-стейджа для target_table (`schema.table` -> `stage_table`)."""
    return "stage_" + target_table.rsplit(".", 1)[-1]


def copy_into_stage(
    cursor,
    batches: Iterable,
    columns: Sequence[str],
    stage_table: str,
    updated_at: datetime,
) -> int:
    """COPY всех батчей во временную таблицу. Целевая таблица не затрагивается.

    Это самая долгая часть публикации — скачивание из S3 и кодирование CSV
    ~10.5М строк, — и она не должна держать блокировку на target_table.
    """
    column_list = ", ".join(list(columns) + ["updated_at"])
    statement = f"COPY {stage_table} ({column_list}) FROM STDIN WITH (FORMAT csv)"
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
    min_rows: int = 1,
) -> int:
    """Заменить содержимое целевой таблицы партицией витрины за одну транзакцию.

    Партиция сперва стадируется во временную таблицу (`COPY`, самая долгая
    часть — скачивание из S3 и кодирование CSV), и только затем target_table
    truncate'ится и заполняется одним `INSERT ... SELECT` из стейджа. ACCESS
    EXCLUSIVE на target_table берётся только на TRUNCATE + INSERT — читатели
    сервиса невыкупов не блокируются на время скана Iceberg.
    """
    from pyiceberg.expressions import EqualTo

    scan = iceberg_table.scan(
        row_filter=EqualTo("date", partition_date),
        selected_fields=tuple(columns),
    )
    stage_table = stage_table_name(target_table)
    connection.autocommit = False
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE TEMP TABLE {stage_table} (LIKE {target_table}) "
                "ON COMMIT DROP"
            )
            written = copy_into_stage(
                cursor,
                scan.to_arrow_batch_reader(),
                columns,
                stage_table,
                updated_at,
            )
            if written == 0:
                # Пустая партиция затёрла бы рабочую таблицу сервиса.
                raise RuntimeError(
                    f"Partition {partition_date} of {iceberg_table.name()} is empty; "
                    f"refusing to leave {target_table} truncated"
                )
            if written < min_rows:
                # Одна хорошая шарда плюс семь пустых тоже проходит written > 0,
                # но это восьмая часть ожидаемого объёма — не короче min_rows.
                raise RuntimeError(
                    f"Partition {partition_date} of {iceberg_table.name()} has "
                    f"{written} rows, below the required minimum of {min_rows}; "
                    f"refusing to leave {target_table} truncated"
                )
            cursor.execute(f"TRUNCATE TABLE {target_table}")
            cursor.execute(f"INSERT INTO {target_table} SELECT * FROM {stage_table}")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    logger.info("Copied %d rows into %s", written, target_table)
    return written
