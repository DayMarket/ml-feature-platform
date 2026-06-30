import importlib.util
import sys
import types
from pathlib import Path


def load_migration_module():
    pyspark_module = types.ModuleType("pyspark")
    pyspark_sql_module = types.ModuleType("pyspark.sql")
    pyspark_sql_module.SparkSession = object
    sys.modules.setdefault("pyspark", pyspark_module)
    sys.modules.setdefault("pyspark.sql", pyspark_sql_module)

    module_path = Path("scripts/run_pyspark_migrations.py")
    spec = importlib.util.spec_from_file_location("run_pyspark_migrations", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeDataType:
    def __init__(self, type_name: str):
        self.type_name = type_name

    def simpleString(self) -> str:
        return self.type_name


class FakeField:
    def __init__(self, name: str, type_name: str):
        self.name = name
        self.dataType = FakeDataType(type_name)


class FakeFrame:
    def __init__(self, fields: list[FakeField]):
        self.schema = type("Schema", (), {"fields": fields})()


class FakeSpark:
    def __init__(self, fields: list[FakeField]):
        self.fields = fields
        self.sql_calls: list[str] = []

    def table(self, _table_name: str) -> FakeFrame:
        return FakeFrame(self.fields)

    def sql(self, statement: str) -> None:
        self.sql_calls.append(statement)


def main() -> int:
    migrations = load_migration_module()
    statement = (
        "ALTER TABLE iceberg.silver.example "
        "RENAME COLUMN IF EXISTS search_queries TO search_queries_with_installs "
        "WHEN SOURCE TYPE IS NOT ARRAY<STRING>"
    )

    fresh_spark = FakeSpark([FakeField("search_queries", "array<string>")])
    migrations.run_statement(fresh_spark, statement)
    assert fresh_spark.sql_calls == []

    old_spark = FakeSpark(
        [
            FakeField(
                "search_queries",
                "array<struct<search_query:string,uniq_installs:bigint>>",
            )
        ]
    )
    migrations.run_statement(old_spark, statement)
    assert old_spark.sql_calls == [
        "ALTER TABLE iceberg.silver.example RENAME COLUMN search_queries TO search_queries_with_installs"
    ]

    migrated_spark = FakeSpark(
        [
            FakeField(
                "search_queries_with_installs",
                "array<struct<search_query:string,uniq_installs:bigint>>",
            ),
            FakeField("search_queries", "array<string>"),
        ]
    )
    migrations.run_statement(migrated_spark, statement)
    assert migrated_spark.sql_calls == []

    print("PySpark migration runner tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
