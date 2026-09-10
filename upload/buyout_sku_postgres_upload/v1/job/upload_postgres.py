"""Публикация партиции витрины sku_buyout_features в PostgreSQL сервиса невыкупов.

Тот же модуль обслуживает и buyout_account_postgres_upload (см. `column_map`
и `constants` в `publish()`) — вторая выгрузка не заводит собственный `job/`,
а импортирует этот файл, как это делают выгрузки, переиспользующие
features_service_upload.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime
from typing import Iterable, Mapping, Sequence

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


# Arrow -> PostgreSQL типы для стейджа, когда стейдж не может быть `LIKE
# target_table` (см. _needs_explicit_stage_schema). Покрывает типы, реально
# встречающиеся в подтверждённых Iceberg-витринах; всё незнакомое падает на
# text — как для любого типа без явного соответствия, потребует явного
# приведения в column_map, если оно вообще идёт в SELECT.
_ARROW_TO_POSTGRES = {
    "bool": "boolean",
    "int16": "smallint",
    "int32": "integer",
    "int64": "bigint",
    "float": "real",
    "double": "double precision",
    "date32[day]": "date",
    "string": "text",
    "large_string": "text",
}


def _postgres_type(arrow_type) -> str:
    return _ARROW_TO_POSTGRES.get(str(arrow_type), "text")


def build_insert_select(
    stage_table: str,
    target_table: str,
    column_map: Mapping[str, str] | None = None,
    constants: Mapping[str, str] | None = None,
) -> str:
    """`INSERT INTO target (...) SELECT ... FROM stage`, или `SELECT *` без обеих карт.

    `column_map` — `{target_column: staged_expression}`. Переименовывает
    колонку стейджа в её целевое имя; значение необязательно голое имя
    колонки стейджа — это может быть произвольное SQL-выражение над
    колонками стейджа (например явный `CAST` под тип целевой колонки).

    `constants` — `{target_column: sql_literal}`. Целевая колонка, у которой
    в стейдже вообще нет источника — значение всегда одно и то же (`true`,
    `''`, ...). Литерал идёт прямо в SQL, а не через CSV/COPY в стейдж:
    `csv.writer` пишет пустую строку как неквотированное пустое поле, а
    `COPY ... FORMAT csv` читает такое поле как SQL NULL, а не как ''. Через
    SQL-литерал в SELECT это подмена исключена.

    Без обеих карт поведение не меняется — тот же `SELECT * FROM stage`, что
    и раньше.
    """
    if not column_map and not constants:
        return f"INSERT INTO {target_table} SELECT * FROM {stage_table}"
    pairs = list((column_map or {}).items())
    pairs += [
        (column, literal)
        for column, literal in (constants or {}).items()
        if column not in (column_map or {})
    ]
    target_columns = ", ".join(column for column, _ in pairs)
    select_list = ", ".join(expression for _, expression in pairs)
    return (
        f"INSERT INTO {target_table} ({target_columns}) "
        f"SELECT {select_list} FROM {stage_table}"
    )


def publish(
    iceberg_table,
    partition_date: date,
    columns: Sequence[str],
    connection,
    target_table: str,
    updated_at: datetime,
    min_rows: int = 1,
    column_map: Mapping[str, str] | None = None,
    constants: Mapping[str, str] | None = None,
) -> int:
    """Заменить содержимое целевой таблицы партицией витрины за одну транзакцию.

    Партиция сперва стадируется во временную таблицу (`COPY`, самая долгая
    часть — скачивание из S3 и кодирование CSV), и только затем target_table
    truncate'ится и заполняется одним `INSERT ... SELECT` из стейджа. ACCESS
    EXCLUSIVE на target_table берётся только на TRUNCATE + INSERT — читатели
    сервиса невыкупов не блокируются на время скана Iceberg.

    `column_map` и `constants` (см. `build_insert_select`) — опциональны и по
    умолчанию `None`; без них поведение байт-в-байт совпадает с прежним:
    `SELECT * FROM stage`, а сам стейдж создаётся как `LIKE target_table`.
    Это годится, только пока имена и типы колонок источника совпадают с
    целевыми (так у buyout_sku_postgres_upload). Как только источник
    переименовывается или приводится к другому типу (buyout_account_postgres_
    upload), стейдж уже не может быть `LIKE target_table` — колонок с такими
    именами там нет, — и создаётся с исходными (не целевыми) именами и
    типами, взятыми из Arrow-схемы скана.
    """
    from pyiceberg.expressions import EqualTo

    scan = iceberg_table.scan(
        row_filter=EqualTo("date", partition_date),
        selected_fields=tuple(columns),
    )
    reader = scan.to_arrow_batch_reader()
    stage_table = stage_table_name(target_table)
    connection.autocommit = False
    try:
        with connection.cursor() as cursor:
            if column_map or constants:
                column_defs = ", ".join(
                    f"{name} {_postgres_type(reader.schema.field(name).type)}"
                    for name in columns
                )
                cursor.execute(
                    f"CREATE TEMP TABLE {stage_table} "
                    f"({column_defs}, updated_at timestamptz) ON COMMIT DROP"
                )
            else:
                cursor.execute(
                    f"CREATE TEMP TABLE {stage_table} (LIKE {target_table}) "
                    "ON COMMIT DROP"
                )
            written = copy_into_stage(
                cursor,
                reader,
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
            cursor.execute(
                build_insert_select(stage_table, target_table, column_map, constants)
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    logger.info("Copied %d rows into %s", written, target_table)
    return written
