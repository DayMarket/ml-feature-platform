"""Контракт выгрузки витрины buyout_online_account_features в PostgreSQL."""

import importlib.util
import json
import sys
import types
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UPLOAD = ROOT / "upload" / "buyout_account_postgres_upload" / "v1"
SKU_UPLOAD = ROOT / "upload" / "buyout_sku_postgres_upload" / "v1"

# Порядок и состав колонок целевой таблицы mlgrowth.account_buyout_features
# (DDL живёт вне репозитория — см. README этой выгрузки).
TARGET_COLUMNS = (
    "account_id",
    "orders_count",
    "no_block",
    "segment_description",
    "text_description_ru",
    "text_description_uz",
    "last_order_date",
    "updated_at",
)

SOURCE_FEATURES = (
    "account_id",
    "orders_created_prev_365d",
    "last_order_date_win",
)


def read_config() -> dict:
    return json.loads((UPLOAD / "config.yaml").read_text(encoding="utf-8"))


def load_job():
    """Загружает job/upload_postgres.py у sku-выгрузки — эта выгрузка своего не заводит."""
    path = SKU_UPLOAD / "job" / "upload_postgres.py"
    spec = importlib.util.spec_from_file_location("upload_postgres_account", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ConfigContract(unittest.TestCase):
    def test_dag_resolves_source_entity_from_repository_root(self):
        source = read_config()["feature_groups"][0]["source"]
        expected_runtime = ROOT / source["entity_path"] / "job" / "runtime.py"

        self.assertTrue(expected_runtime.is_file())
        dag_source = (UPLOAD / "dag.py").read_text(encoding="utf-8")
        self.assertIn(
            'REPO_ROOT = os.path.abspath(os.path.join(UPLOAD_DIR, "..", "..", ".."))',
            dag_source,
        )

    def test_declares_a_postgres_sink(self):
        config = read_config()
        sink = config["sink"]
        self.assertEqual(sink["type"], "postgres")
        self.assertEqual(sink["connection_id"], "postgres_non_buyout_service_connect")
        self.assertEqual(sink["schema"], "mlgrowth")
        self.assertEqual(sink["table"], "account_buyout_features")

    def test_waits_for_the_gold_dq_task_with_an_hour_offset(self):
        source = read_config()["feature_groups"][0]["source"]
        self.assertEqual(source["dependency_task_id"], "dq")
        self.assertEqual(
            source["dependency_dag_id"],
            "feature-platform.layers.gold.account_id.buyout_online_account_features",
        )
        self.assertEqual(source["dependency_execution_delta_minutes"], 60)

    def test_features_are_exactly_the_source_columns(self):
        # sink.column_map/constants делают переименование и константы — здесь
        # только исходные имена колонок витрины, как их сверяет валидатор
        # с миграциями источника.
        features = read_config()["feature_groups"][0]["features"]
        self.assertEqual(tuple(features), SOURCE_FEATURES)

    def test_has_no_source_limit(self):
        self.assertNotIn("limit", read_config()["feature_groups"][0]["source"])


class InsertSelectBuilder(unittest.TestCase):
    """Строит явный список колонок INSERT из sink.column_map + sink.constants."""

    def test_builds_explicit_insert_naming_all_eight_target_columns(self):
        job = load_job()
        sink = read_config()["sink"]

        sql = job.build_insert_select(
            "stage_account_buyout_features",
            "mlgrowth.account_buyout_features",
            sink["column_map"],
            sink["constants"],
        )

        self.assertEqual(
            sql,
            "INSERT INTO mlgrowth.account_buyout_features "
            "(account_id, orders_count, last_order_date, updated_at, "
            "no_block, segment_description, text_description_ru, text_description_uz) "
            "SELECT account_id, orders_created_prev_365d, "
            "CAST(last_order_date_win AS timestamptz), updated_at, "
            "true, '', '', '' "
            "FROM stage_account_buyout_features",
        )

        # Целевые колонки — ровно восемь, ровно колонки DDL.
        inserted_columns = sql.split("(", 1)[1].split(")", 1)[0]
        self.assertEqual(
            tuple(c.strip() for c in inserted_columns.split(",")),
            (
                "account_id",
                "orders_count",
                "last_order_date",
                "updated_at",
                "no_block",
                "segment_description",
                "text_description_ru",
                "text_description_uz",
            ),
        )
        self.assertEqual(set(inserted_columns.replace(" ", "").split(",")), set(TARGET_COLUMNS))

    def test_constants_are_not_among_the_copied_columns(self):
        # Стейдж и COPY знают только про SOURCE_FEATURES (+ updated_at) —
        # константные колонки в витрине не существуют и через COPY не идут.
        copy_columns = ", ".join(list(SOURCE_FEATURES) + ["updated_at"])
        for constant_column in read_config()["sink"]["constants"]:
            self.assertNotIn(constant_column, copy_columns)


class _FakeEqualTo:
    """Stand-in for pyiceberg.expressions.EqualTo — value is never inspected."""

    def __init__(self, field, value):
        self.field = field
        self.value = value


class _RecordingCursor:
    def __init__(self, record):
        self._record = record

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql):
        self._record.append(("execute", sql))

    def copy_expert(self, statement, buffer):
        self._record.append(("copy_expert", statement))


class _RecordingConnection:
    def __init__(self, record):
        self._record = record
        self._autocommit = None

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self._autocommit = value
        self._record.append(("autocommit", value))

    def cursor(self):
        return _RecordingCursor(self._record)

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
        import pyarrow as pa

        schema = pa.schema(
            [
                pa.field("account_id", pa.int64()),
                pa.field("orders_created_prev_365d", pa.int64()),
                pa.field("last_order_date_win", pa.date32()),
            ]
        )
        return pa.RecordBatchReader.from_batches(schema, iter(self._batches))


class _FakeIcebergTable:
    def __init__(self, batches, name="gold.feature_platform_buyout_online_account_features"):
        self._batches = batches
        self._name = name

    def scan(self, row_filter, selected_fields):
        return _FakeScan(self._batches)

    def name(self):
        return self._name


class PublishTransactionContract(unittest.TestCase):
    """publish() через фейки: стейдж -> COPY -> проверка объёма -> TRUNCATE -> INSERT -> commit."""

    STAMP = datetime(2026, 9, 10, 7, 0, tzinfo=timezone.utc)
    TARGET_TABLE = "mlgrowth.account_buyout_features"

    def setUp(self):
        self.job = load_job()
        self.sink = read_config()["sink"]
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
    def _batches(account_id_chunks):
        import pyarrow as pa

        return [
            pa.RecordBatch.from_pydict(
                {
                    "account_id": chunk,
                    "orders_created_prev_365d": [1] * len(chunk),
                    "last_order_date_win": [date(2026, 9, 9)] * len(chunk),
                }
            )
            for chunk in account_id_chunks
        ]

    @staticmethod
    def _executed_sql(record):
        return [entry[1] for entry in record if entry[0] == "execute"]

    def test_normal_path_stages_then_truncates_then_inserts_then_commits(self):
        record = []
        connection = _RecordingConnection(record)
        table = _FakeIcebergTable(self._batches([[1, 2], [3]]))

        written = self.job.publish(
            table,
            date(2026, 9, 9),
            SOURCE_FEATURES,
            connection,
            self.TARGET_TABLE,
            self.STAMP,
            min_rows=1,
            column_map=self.sink["column_map"],
            constants=self.sink["constants"],
        )

        self.assertEqual(written, 3)
        kinds = [entry[0] for entry in record]
        self.assertEqual(
            kinds,
            [
                "autocommit",
                "execute",  # CREATE TEMP TABLE stage (исходные имена/типы колонок)
                "copy_expert",
                "copy_expert",
                "execute",  # TRUNCATE TABLE target
                "execute",  # INSERT INTO target (...) SELECT ... FROM stage
                "commit",
                "close",
            ],
        )
        self.assertEqual(record[0], ("autocommit", False))
        executed = self._executed_sql(record)
        self.assertTrue(executed[0].startswith("CREATE TEMP TABLE"))
        self.assertIn("ON COMMIT DROP", executed[0])
        # Стейдж не LIKE target_table: у него исходные имена колонок.
        self.assertIn("last_order_date_win", executed[0])
        self.assertNotIn("orders_count", executed[0])
        self.assertTrue(executed[1].startswith(f"TRUNCATE TABLE {self.TARGET_TABLE}"))
        self.assertTrue(executed[2].startswith(f"INSERT INTO {self.TARGET_TABLE} ("))
        self.assertIn("CAST(last_order_date_win AS timestamptz)", executed[2])
        self.assertIn("true, '', '', ''", executed[2])
        copy_index = kinds.index("copy_expert")
        truncate_index = kinds.index("execute", copy_index)
        self.assertGreater(truncate_index, copy_index)
        self.assertNotIn("rollback", kinds)

    def test_empty_partition_rolls_back_and_never_commits(self):
        record = []
        connection = _RecordingConnection(record)
        table = _FakeIcebergTable(self._batches([]))

        with self.assertRaises(RuntimeError):
            self.job.publish(
                table,
                date(2026, 9, 9),
                SOURCE_FEATURES,
                connection,
                self.TARGET_TABLE,
                self.STAMP,
                min_rows=1,
                column_map=self.sink["column_map"],
                constants=self.sink["constants"],
            )

        kinds = [entry[0] for entry in record]
        self.assertIn("rollback", kinds)
        self.assertNotIn("commit", kinds)
        self.assertEqual(kinds[-1], "close")
        self.assertFalse(
            any(sql.startswith("TRUNCATE") for sql in self._executed_sql(record))
        )


if __name__ == "__main__":
    unittest.main()
