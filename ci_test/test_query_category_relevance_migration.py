"""Миграция query_id bigint->string: прогон через раннер на состоянии прода и повторно."""

import re
import sys
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_run_pyspark_migrations import FakeField, FakeSpark, load_migration_module

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/gold/category_id_query_text/query_category_relevance/v1"
MIGRATION = ENTITY / "migrations/20260914_query_id_to_string.sql"
TARGET_TABLE = "iceberg.gold.feature_platform_query_category_relevance"

# Схема таблицы в проде на 2026-09-14, снятая из information_schema.
PRODUCTION_FIELDS = (
    ("date", "date"),
    ("query_id", "bigint"),
    ("query_text", "string"),
    ("category_id", "bigint"),
    ("relevance", "int"),
)


class SchemaTrackingSpark(FakeSpark):
    """FakeSpark, применяющий DDL к своей схеме.

    Базовый фейк возвращает исходный список колонок и после RENAME: второй оператор
    файла увидел бы старое имя и решил, что добавлять нечего. Настоящий Spark читает
    схему заново, поэтому проверять последовательность операторов одного файла можно
    только на фейке, который её обновляет.
    """

    def sql(self, statement: str):
        result = super().sql(statement)
        rename = re.search(
            r"RENAME COLUMN (?P<column>\w+) TO (?P<new_column>\w+)$", statement
        )
        if rename:
            for index, field in enumerate(self.fields):
                if field.name == rename.group("column"):
                    self.fields[index] = FakeField(
                        rename.group("new_column"), field.dataType.simpleString()
                    )
            return result
        drop = re.search(r"DROP COLUMN (?P<column>\w+)$", statement)
        if drop:
            self.fields = [
                field for field in self.fields if field.name != drop.group("column")
            ]
            return result
        add = re.search(r"ADD COLUMN (?P<column>\w+) (?P<type>\w+)", statement)
        if add:
            self.fields.append(FakeField(add.group("column"), add.group("type").lower()))
        return result


@contextmanager
def _isolated_pyspark_stub():
    """Вернуть sys.modules в исходное состояние после загрузки раннера.

    load_migration_module подменяет pyspark и pyspark.sql заглушками. В общем процессе
    pytest заглушка переживает модуль и ломает импорт у тестов, которым нужен настоящий
    pyspark.sql.functions, — 19 ошибок сбора в ci_test/test_query_id_features.py.
    """
    names = ("pyspark", "pyspark.sql")
    saved = {name: sys.modules.get(name) for name in names}
    try:
        yield
    finally:
        for name, module in saved.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


def _run(fields: list[FakeField]) -> FakeSpark:
    with _isolated_pyspark_stub():
        migrations = load_migration_module()
    spark = SchemaTrackingSpark(fields)
    sql = MIGRATION.read_text(encoding="utf-8").format(target_table=TARGET_TABLE)
    for statement in migrations.split_sql(sql):
        migrations.run_statement(spark, statement)
    return spark


def test_migration_recreates_query_id_as_string_on_the_production_schema():
    spark = _run([FakeField(name, type_name) for name, type_name in PRODUCTION_FIELDS])

    assert spark.sql_calls == [
        f"ALTER TABLE {TARGET_TABLE} RENAME COLUMN query_id TO query_id_bigint",
        f"ALTER TABLE {TARGET_TABLE} ADD COLUMN query_id STRING "
        "COMMENT 'ID поискового запроса; строка, а не число (в Trino — varchar)'",
        f"ALTER TABLE {TARGET_TABLE} DROP COLUMN query_id_bigint",
    ]


def test_migration_leaves_no_temporary_column_behind():
    """Промежуточная query_id_bigint не должна пережить миграцию."""
    spark = _run([FakeField(name, type_name) for name, type_name in PRODUCTION_FIELDS])

    schema = {field.name: field.dataType.simpleString() for field in spark.fields}
    assert "query_id_bigint" not in schema
    assert schema["query_id"] == "string"
    assert set(schema) == {"date", "query_id", "query_text", "category_id", "relevance"}


def test_migration_is_idempotent_on_its_own_result():
    """Второй прогон не должен ни трогать строковую колонку, ни удалять что-либо."""
    migrated = [FakeField(name, type_name) for name, type_name in PRODUCTION_FIELDS]
    migrated[1] = FakeField("query_id", "string")

    assert _run(migrated).sql_calls == []


def test_migration_leaves_a_fresh_table_untouched():
    """На таблице из create_table.sql колонка уже строковая — миграция ничего не делает."""
    fresh = [FakeField(name, type_name) for name, type_name in PRODUCTION_FIELDS]
    fresh[1] = FakeField("query_id", "string")

    assert _run(fresh).sql_calls == []


def test_create_table_declares_query_id_as_string():
    """create_table.sql и миграция обязаны сходиться в одном типе."""
    create_table = (ENTITY / "migrations/create_table.sql").read_text(encoding="utf-8")

    assert "query_id STRING" in create_table
    assert "query_id BIGINT" not in create_table


def test_drop_column_requires_if_exists():
    """Без IF EXISTS повторный прогон падал бы на уже удалённой колонке."""
    with _isolated_pyspark_stub():
        migrations = load_migration_module()

    migrations.validate_idempotent_statement(
        f"ALTER TABLE {TARGET_TABLE} DROP COLUMN IF EXISTS query_id_bigint",
        MIGRATION,
    )
    try:
        migrations.validate_idempotent_statement(
            f"ALTER TABLE {TARGET_TABLE} DROP COLUMN query_id_bigint",
            MIGRATION,
        )
    except RuntimeError as error:
        assert "IF EXISTS" in str(error)
    else:
        raise AssertionError("DROP COLUMN без IF EXISTS должен отклоняться")


def test_dropping_a_whole_table_is_still_rejected():
    """Послабление касается только колонки: DROP TABLE и DELETE остаются запрещены."""
    with _isolated_pyspark_stub():
        migrations = load_migration_module()

    for statement in (
        f"DROP TABLE IF EXISTS {TARGET_TABLE}",
        f"DELETE FROM {TARGET_TABLE}",
        f"TRUNCATE TABLE {TARGET_TABLE}",
    ):
        try:
            migrations.validate_idempotent_statement(statement, MIGRATION)
        except RuntimeError as error:
            assert "destructive" in str(error)
        else:
            raise AssertionError(f"{statement} должен отклоняться")


def main() -> int:
    test_migration_recreates_query_id_as_string_on_the_production_schema()
    test_migration_leaves_no_temporary_column_behind()
    test_migration_is_idempotent_on_its_own_result()
    test_drop_column_requires_if_exists()
    test_dropping_a_whole_table_is_still_rejected()
    test_migration_leaves_a_fresh_table_untouched()
    test_create_table_declares_query_id_as_string()
    print("Query category relevance migration tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
