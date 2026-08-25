import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feature_stats.task import TASK_ID, build_stats_context, fetch_rows, partition_instant

DAILY_CONFIG = {
    "table": {
        "catalog": "iceberg",
        "schema": "gold",
        "name": "feature_platform_sku_group_price_features",
        "primary_key": "date,sku_group_id",
        "meta": {"team": "team:search"},
    },
    "feature_stats": {"exclude_columns": []},
}

SNAPSHOT_CONFIG = {
    "table": {
        "catalog": "iceberg",
        "schema": "gold",
        "name": "feature_platform_dynamic_pricing_sku_group_price_features",
        "primary_key": "calculated_at,sku_group_id,promotion_id",
        "meta": {"team": "team:search"},
    },
    "feature_stats": {
        "partition_granularity": "timestamp",
        "partition_column": "calculated_at",
        "snapshot_interval_hours": 3,
        "partition_date_template": "{{ x }}",
    },
}


def test_task_id_is_stable() -> None:
    # На это имя опирается wiring-тест; downstream-сенсоры на него вешать нельзя.
    assert TASK_ID == "feature_stats"


def test_partition_instant_for_a_daily_entity_is_midnight_utc() -> None:
    assert partition_instant(date(2026, 8, 22), None) == datetime(2026, 8, 22, tzinfo=timezone.utc)


def test_partition_instant_for_a_snapshot_entity_is_the_snapshot() -> None:
    naive = datetime(2026, 8, 22, 6, 0, 0)
    assert partition_instant(date(2026, 8, 22), naive) == datetime(
        2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc
    )


def test_build_stats_context_daily() -> None:
    ctx = build_stats_context(DAILY_CONFIG, Path("."), "2026-08-22")
    assert ctx.render.catalog_alias == "dwh-iceberg"
    assert ctx.render.schema == "gold"
    assert ctx.render.table == "feature_platform_sku_group_price_features"
    assert ctx.render.primary_key == ("date", "sku_group_id")
    assert ctx.render.partition_column == "date"
    assert ctx.render.partition_date == date(2026, 8, 22)
    assert ctx.render.partition_granularity == "date"
    assert ctx.render.team == "team:search"
    assert ctx.partition_ts == datetime(2026, 8, 22, tzinfo=timezone.utc)


def test_build_stats_context_snapshot() -> None:
    ctx = build_stats_context(SNAPSHOT_CONFIG, Path("."), "2026-08-22 06:00:00")
    assert ctx.render.partition_column == "calculated_at"
    assert ctx.render.partition_granularity == "timestamp"
    assert ctx.render.partition_timestamp == datetime(2026, 8, 22, 6, 0, 0)
    assert ctx.render.snapshot_interval_hours == 3
    assert ctx.partition_ts == datetime(2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc)


def test_build_stats_context_defaults_the_team() -> None:
    config = {"table": {**DAILY_CONFIG["table"]}}
    config["table"].pop("meta")
    assert build_stats_context(config, Path("."), "2026-08-22").render.team == "team:search"


class FakeCursor:
    def __init__(self, rows: list) -> None:
        self.rows = rows
        self.executed: list[str] = []
        self.closed = False

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchall(self) -> list:
        return self.rows

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, rows: list) -> None:
        self.cursor_object = FakeCursor(rows)
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.cursor_object

    def close(self) -> None:
        self.closed = True


class FakeHook:
    """TrinoHook, у которого обращение к get_records — это провал теста."""

    def __init__(self, rows: list) -> None:
        self.connection = FakeConnection(rows)

    def get_conn(self) -> FakeConnection:
        return self.connection

    def get_records(self, sql: str) -> list:
        raise AssertionError(
            "feature_stats снова читает через hook.get_records: тот гоняет SQL через "
            "sqlparse.split_sql_string, а sqlparse отказывается разбирать больше 10000 "
            "токенов — профиль широкой таблицы падает, не доехав до Trino"
        )


def test_fetch_rows_executes_the_statement_on_a_cursor() -> None:
    hook = FakeHook([[1, 2, 3]])
    assert fetch_rows(hook, "SELECT 1, 2, 3") == [[1, 2, 3]]
    assert hook.connection.cursor_object.executed == ["SELECT 1, 2, 3"]


def test_fetch_rows_closes_the_cursor_and_the_connection() -> None:
    # Без закрытия каждый прогон таски оставляет висеть сессию Trino.
    hook = FakeHook([[0]])
    fetch_rows(hook, "SELECT 0")
    assert hook.connection.cursor_object.closed
    assert hook.connection.closed


def test_fetch_rows_closes_everything_even_when_trino_fails() -> None:
    class ExplodingCursor(FakeCursor):
        def execute(self, sql: str) -> None:
            raise RuntimeError("Trino сказал нет")

    hook = FakeHook([])
    hook.connection.cursor_object = ExplodingCursor([])
    try:
        fetch_rows(hook, "SELECT 1")
    except RuntimeError:
        pass
    else:
        raise AssertionError("ошибка Trino обязана долетать до таски")
    assert hook.connection.cursor_object.closed
    assert hook.connection.closed


def main() -> int:
    test_task_id_is_stable()
    test_partition_instant_for_a_daily_entity_is_midnight_utc()
    test_partition_instant_for_a_snapshot_entity_is_the_snapshot()
    test_build_stats_context_daily()
    test_build_stats_context_snapshot()
    test_build_stats_context_defaults_the_team()
    test_fetch_rows_executes_the_statement_on_a_cursor()
    test_fetch_rows_closes_the_cursor_and_the_connection()
    test_fetch_rows_closes_everything_even_when_trino_fails()
    print("Feature stats task tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
