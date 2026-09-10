"""Контракт выгрузки витрины sku_buyout_features в PostgreSQL."""

import importlib.util
import io
import json
import sys
import types
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UPLOAD = ROOT / "upload" / "buyout_sku_postgres_upload" / "v1"

# Порядок и состав колонок целевой таблицы mlgrowth.sku_buyout_features.
# category_id в неё не выгружается.
TARGET_COLUMNS = (
    "sku_id",
    "product_id",
    "seller_id",
    "l1_category",
    "l2_category",
    "l3_category",
    "l4_category",
    "l5_category",
    "type",
    "commission",
    "cost_price",
    "predicted_dimensional_group",
    "is_not_block",
    "sku_buyout",
    "product_buyout",
    "category_buyout",
    "shop_buyout",
    "category_no_show",
    "sku_n_delivered",
    "product_n_delivered",
)


def read_config() -> dict:
    return json.loads((UPLOAD / "config.yaml").read_text(encoding="utf-8"))


def load_job():
    path = UPLOAD / "job" / "upload_postgres.py"
    spec = importlib.util.spec_from_file_location("upload_postgres", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ConfigContract(unittest.TestCase):
    def test_declares_a_postgres_sink(self):
        config = read_config()
        self.assertEqual(config["sink"]["type"], "postgres")
        self.assertEqual(
            config["sink"]["connection_id"], "postgres_non_buyout_service_connect"
        )
        self.assertEqual(config["sink"]["schema"], "mlgrowth")
        self.assertEqual(config["sink"]["table"], "sku_buyout_features")

    def test_waits_for_the_gold_dq_task(self):
        source = read_config()["feature_groups"][0]["source"]
        self.assertEqual(source["dependency_task_id"], "dq")
        self.assertEqual(
            source["dependency_dag_id"],
            "feature-platform.layers.gold.sku_id.sku_buyout_features",
        )
        self.assertEqual(source["dependency_execution_delta_minutes"], 0)

    def test_does_not_publish_category_id(self):
        features = read_config()["feature_groups"][0]["features"]
        self.assertNotIn("category_id", features)
        self.assertEqual(tuple(features), TARGET_COLUMNS)

    def test_has_no_source_limit(self):
        self.assertNotIn("limit", read_config()["feature_groups"][0]["source"])


class CsvSerialisation(unittest.TestCase):
    def test_nulls_stay_null_and_are_not_zero_filled(self):
        # Пропуск в выкупаемости — законное значение, нулём его заменять нельзя.
        import pyarrow as pa

        job = load_job()
        batch = pa.RecordBatch.from_pydict(
            {"sku_id": [1], "sku_buyout": [None]},
        )
        stamp = datetime(2026, 9, 10, 7, 0, tzinfo=timezone.utc)
        buffer = job.batch_to_csv(batch, ("sku_id", "sku_buyout"), stamp)
        row = buffer.getvalue().strip()
        self.assertEqual(row, "1,,2026-09-10 07:00:00+00:00")

    def test_appends_the_run_timestamp_as_last_column(self):
        import pyarrow as pa

        job = load_job()
        batch = pa.RecordBatch.from_pydict({"sku_id": [7]})
        stamp = datetime(2026, 9, 10, 7, 0, tzinfo=timezone.utc)
        buffer = job.batch_to_csv(batch, ("sku_id",), stamp)
        self.assertTrue(buffer.getvalue().strip().endswith("2026-09-10 07:00:00+00:00"))


class _FakeEqualTo:
    """Stand-in for pyiceberg.expressions.EqualTo — value is never inspected."""

    def __init__(self, field, value):
        self.field = field
        self.value = value


class _RecordingCursor:
    """Cursor fake: records execute()/copy_expert() calls into a shared list.

    Usable as a context manager, like a real psycopg2 cursor.
    """

    def __init__(self, record, fail_copy_expert_at_call=None):
        self._record = record
        self._fail_copy_expert_at_call = fail_copy_expert_at_call
        self._copy_expert_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql):
        self._record.append(("execute", sql))

    def copy_expert(self, statement, buffer):
        self._copy_expert_calls += 1
        if self._copy_expert_calls == self._fail_copy_expert_at_call:
            raise RuntimeError("copy_expert failed")
        self._record.append(("copy_expert", statement))


class _RecordingConnection:
    """Connection fake: records autocommit/commit/rollback/close into one list."""

    def __init__(self, record, fail_copy_expert_at_call=None):
        self._record = record
        self._fail_copy_expert_at_call = fail_copy_expert_at_call
        self._autocommit = None

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self._autocommit = value
        self._record.append(("autocommit", value))

    def cursor(self):
        return _RecordingCursor(self._record, self._fail_copy_expert_at_call)

    def commit(self):
        self._record.append(("commit",))

    def rollback(self):
        self._record.append(("rollback",))

    def close(self):
        self._record.append(("close",))


class _FakeScan:
    def __init__(self, batches):
        self._batches = batches

    def to_arrow_batch_reader(self):
        return iter(self._batches)


class _FakeIcebergTable:
    def __init__(self, batches, name="gold.feature_platform_sku_buyout_features"):
        self._batches = batches
        self._name = name

    def scan(self, row_filter, selected_fields):
        return _FakeScan(self._batches)

    def name(self):
        return self._name


class PublishTransactionContract(unittest.TestCase):
    """Drives publish() against fakes to verify order, scope and control flow.

    A grep over the source cannot see whether commit() runs before or after
    the empty-partition guard, or whether TRUNCATE and COPY share a
    connection — only actually calling publish() can.
    """

    STAMP = datetime(2026, 9, 10, 7, 0, tzinfo=timezone.utc)
    TARGET_TABLE = "mlgrowth.sku_buyout_features"

    def setUp(self):
        self.job = load_job()
        # publish() does `from pyiceberg.expressions import EqualTo` internally;
        # pyiceberg is a runtime-image dependency, not installed in this test
        # environment, so it is stubbed in sys.modules for the duration of the test.
        self._previous_pyiceberg = sys.modules.get("pyiceberg")
        self._previous_expressions = sys.modules.get("pyiceberg.expressions")
        pyiceberg_module = types.ModuleType("pyiceberg")
        expressions_module = types.ModuleType("pyiceberg.expressions")
        expressions_module.EqualTo = _FakeEqualTo
        sys.modules["pyiceberg"] = pyiceberg_module
        sys.modules["pyiceberg.expressions"] = expressions_module

    def tearDown(self):
        if self._previous_pyiceberg is None:
            sys.modules.pop("pyiceberg", None)
        else:
            sys.modules["pyiceberg"] = self._previous_pyiceberg
        if self._previous_expressions is None:
            sys.modules.pop("pyiceberg.expressions", None)
        else:
            sys.modules["pyiceberg.expressions"] = self._previous_expressions

    @staticmethod
    def _batches(row_id_chunks):
        import pyarrow as pa

        return [pa.RecordBatch.from_pydict({"sku_id": chunk}) for chunk in row_id_chunks]

    def test_normal_path_truncates_then_copies_then_commits(self):
        record = []
        connection = _RecordingConnection(record)
        table = _FakeIcebergTable(self._batches([[1, 2], [3]]))

        written = self.job.publish(
            table,
            date(2026, 9, 9),
            ("sku_id",),
            connection,
            self.TARGET_TABLE,
            self.STAMP,
        )

        self.assertEqual(written, 3)
        kinds = [entry[0] for entry in record]
        self.assertEqual(
            kinds,
            ["autocommit", "execute", "copy_expert", "copy_expert", "commit", "close"],
        )
        self.assertEqual(record[0], ("autocommit", False))
        self.assertTrue(record[1][1].startswith("TRUNCATE TABLE"))
        self.assertNotIn("rollback", kinds)

    def test_empty_partition_rolls_back_and_never_commits(self):
        # The scenario that would otherwise leave the service's table truncated.
        record = []
        connection = _RecordingConnection(record)
        table = _FakeIcebergTable(self._batches([]))

        with self.assertRaises(RuntimeError):
            self.job.publish(
                table,
                date(2026, 9, 9),
                ("sku_id",),
                connection,
                self.TARGET_TABLE,
                self.STAMP,
            )

        kinds = [entry[0] for entry in record]
        self.assertIn("rollback", kinds)
        self.assertNotIn("commit", kinds)
        self.assertIn("close", kinds)
        self.assertEqual(kinds[-1], "close")

    def test_mid_copy_failure_rolls_back_and_never_commits(self):
        record = []
        connection = _RecordingConnection(record, fail_copy_expert_at_call=2)
        table = _FakeIcebergTable(self._batches([[1], [2], [3]]))

        with self.assertRaises(RuntimeError):
            self.job.publish(
                table,
                date(2026, 9, 9),
                ("sku_id",),
                connection,
                self.TARGET_TABLE,
                self.STAMP,
            )

        kinds = [entry[0] for entry in record]
        self.assertIn("rollback", kinds)
        self.assertNotIn("commit", kinds)
        self.assertIn("close", kinds)
        self.assertEqual(kinds[-1], "close")
        # Only the first batch's copy_expert made it into the record; the
        # second batch is where the fake raised.
        self.assertEqual(kinds.count("copy_expert"), 1)


if __name__ == "__main__":
    unittest.main()
