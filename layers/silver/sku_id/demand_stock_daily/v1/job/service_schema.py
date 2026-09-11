"""Сверить физическую схему DQ/stats с миграцией до извлечения фактов."""

from pathlib import Path
import re

import pyarrow as pa


def service_schema(entity_path):
    """Читать простые колонки текущей DQ/stats миграции, неизвестный DDL отклонять."""
    ddl = (Path(entity_path) / "migrations/create_table.sql").read_text(encoding="utf-8")
    body = re.search(r"CREATE TABLE IF NOT EXISTS \{target_table\}\s*\((.*?)\n\)\s*USING iceberg", ddl, re.S)
    if body is None:
        raise ValueError("Неподдержанная форма service migration")
    types = {"DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"), "STRING": pa.string(),
             "INT": pa.int32(), "BIGINT": pa.int64(), "DOUBLE": pa.float64(), "BOOLEAN": pa.bool_()}
    fields = []
    for line in body.group(1).splitlines():
        if not line.strip():
            continue
        column = re.fullmatch(r"\s*([a-z_][a-z0-9_]*)\s+([A-Z]+)( NOT NULL)? COMMENT '(?:''|[^'])*',?\s*", line)
        if column is None or column[2] not in types:
            raise ValueError("Неподдержанная колонка service migration")
        fields.append(pa.field(column[1], types[column[2]], nullable=not bool(column[3])))
    if not fields or len({field.name for field in fields}) != len(fields):
        raise ValueError("Пустая/неоднозначная service schema")
    return pa.schema(fields)


def same_type(actual, expected):
    return actual == expected or pa.types.is_string(expected) and pa.types.is_large_string(actual)


def validate_service_schema(actual, expected):
    if len(actual) != len(expected) or set(actual.names) != set(expected.names):
        raise ValueError("Service schema не соответствует миграции")
    for field in expected:
        found = actual.field(field.name)
        if not same_type(found.type, field.type) or found.nullable != field.nullable:
            raise ValueError(f"Неверный тип/nullable service.{field.name}")
