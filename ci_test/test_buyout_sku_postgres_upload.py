"""Контракт выгрузки витрины sku_buyout_features в PostgreSQL."""

import importlib.util
import io
import json
import unittest
from datetime import datetime, timezone
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


class TransactionContract(unittest.TestCase):
    def test_truncate_and_copy_share_one_transaction(self):
        source = (UPLOAD / "job" / "upload_postgres.py").read_text(encoding="utf-8")
        self.assertIn("TRUNCATE TABLE", source)
        self.assertIn("conn.commit()", source)
        self.assertIn("conn.rollback()", source)
        self.assertNotIn("autocommit = True", source)


if __name__ == "__main__":
    unittest.main()
