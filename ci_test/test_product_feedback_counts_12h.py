import importlib.util
import sys
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTITY_PATH = (
    ROOT
    / "layers"
    / "silver"
    / "product_id"
    / "product_feedback_counts_12h"
    / "v1"
)


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@dataclass(frozen=True)
class QuerySettings:
    feedback_table: str = "iceberg.silver_bxappdb2_foodback.public_feedback"
    published_status: str = "PUBLISHED"
    window_hours: int = 12


class ProductFeedbackCountsQueryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.query = load_module(
            ENTITY_PATH / "job" / "query.py",
            "test_product_feedback_counts_query",
        )
        cls.settings = QuerySettings()

    def build_query(self):
        return self.query.build_product_feedback_counts_query(
            self.settings,
            datetime(2026, 8, 5, 19, 0, tzinfo=timezone.utc),
        )

    def test_query_uses_half_open_12_hour_window(self):
        sql = self.build_query()

        self.assertIn(
            "date_published >= TIMESTAMP '2026-08-05 12:00:00'",
            sql,
        )
        self.assertIn(
            "date_published < TIMESTAMP '2026-08-06 00:00:00'",
            sql,
        )
        self.assertIn(
            "TIMESTAMP '2026-08-06 00:00:00' AS calculated_at",
            sql,
        )
        self.assertNotIn("date_published <=", sql)

    def test_query_filters_published_feedbacks_and_products(self):
        sql = self.build_query()

        self.assertIn("status = 'PUBLISHED'", sql)
        self.assertIn("CAST(product_id AS INT) > 0", sql)
        self.assertNotIn("BETWEEN 1 AND 5", sql)
        self.assertIn("CAST(rating AS INT) >= 4", sql)
        self.assertIn("CAST(rating AS INT) <= 3", sql)
        self.assertNotIn("silver.sku", sql)

    def test_query_builds_additive_counts_only(self):
        sql = self.build_query()

        for column in (
            "feedback_count",
            "rating_sum",
            "feedback_gte_4",
            "feedback_lte_3",
        ):
            self.assertIn(column, sql)
        self.assertNotIn("rated_feedback_count", sql)
        self.assertNotIn("AVG(", sql)


class ProductFeedbackCountsRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.partition = load_module(
            ENTITY_PATH / "job" / "partition.py",
            "test_product_feedback_counts_partition",
        )
        cls.runtime_config = load_module(
            ENTITY_PATH / "job" / "runtime_config.py",
            "test_product_feedback_counts_runtime_config",
        )

    def test_partition_parser_accepts_airflow_timestamp_formats(self):
        values = (
            "2026-08-05T19:00:00",
            "2026-08-05T19:00:00+00:00",
            "2026-08-05T19:00:00Z",
            "2026-08-05 19:00:00+00:00",
            "2026-08-05 19:00:00",
        )

        for value in values:
            with self.subTest(value=value):
                self.assertEqual(
                    self.partition.parse_airflow_timestamp(value),
                    datetime(2026, 8, 5, 19, 0, tzinfo=timezone.utc),
                )

    def test_partition_parser_normalizes_timezone_to_utc(self):
        parsed = self.partition.parse_airflow_timestamp("2026-08-06T00:00:00+05:00")

        self.assertEqual(
            parsed,
            datetime(2026, 8, 5, 19, 0, tzinfo=timezone.utc),
        )

    def test_partition_parser_rejects_invalid_value(self):
        with self.assertRaisesRegex(ValueError, "not-a-timestamp"):
            self.partition.parse_airflow_timestamp("not-a-timestamp")

    def test_runtime_config_matches_contract(self):
        settings = self.runtime_config.load_source_settings(ENTITY_PATH / "config.yaml")

        self.assertEqual(
            settings.feedback_table,
            "iceberg.silver_bxappdb2_foodback.public_feedback",
        )
        self.assertEqual(settings.published_status, "PUBLISHED")
        self.assertEqual(settings.window_hours, 12)

        config = (ENTITY_PATH / "config.yaml").read_text(encoding="utf-8")
        for field_name in (
            "min_rating",
            "max_rating",
            "positive_rating_min",
            "negative_rating_max",
        ):
            self.assertNotIn(field_name, config)


class ProductFeedbackCountsMigrationTest(unittest.TestCase):
    def test_output_schema_contains_only_contract_columns(self):
        migration = (ENTITY_PATH / "migrations" / "create_table.sql").read_text(
            encoding="utf-8"
        )

        for column in (
            "calculated_at TIMESTAMP",
            "product_id INT",
            "feedback_count BIGINT",
            "rating_sum BIGINT",
            "feedback_gte_4 BIGINT",
            "feedback_lte_3 BIGINT",
        ):
            self.assertIn(column, migration)
        self.assertNotIn("rated_feedback_count", migration)
        self.assertNotIn("sku_id", migration)
        self.assertNotIn("sku_group_id", migration)
        self.assertIn("PARTITIONED BY (hours(calculated_at))", migration)


if __name__ == "__main__":
    unittest.main()
