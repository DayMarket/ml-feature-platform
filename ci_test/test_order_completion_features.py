import importlib.util
import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
SILVER = ROOT / "layers" / "silver"
ENTITIES = {
    "city": SILVER / "order_city_id" / "order_completion_city_features" / "v1",
    "region": SILVER / "order_region_id" / "order_completion_region_features" / "v1",
    "category": (
        SILVER
        / "category_level_category_id"
        / "order_completion_category_features"
        / "v1"
    ),
}
GROUP_TAG = "order-completion-rates"
EXCLUDED_RETURN_CAUSES = (
    "MISSING",
    "DEFECTED",
    "BAD_QUALITY",
    "WRONG_ITEM",
    "PHOTO_MISMATCH",
    "CONTENT",
)


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class FakeCatalog:
    def __init__(self, exists=True):
        self.exists = exists
        self.identifiers = []

    def table_exists(self, identifier):
        self.identifiers.append(("exists", identifier))
        return self.exists

    def load_table(self, identifier):
        self.identifiers.append(("load", identifier))
        return object()


class OrderCompletionPartitionDateTest(unittest.TestCase):
    """Partition date parsing must accept every interval format Airflow can emit."""

    def setUp(self):
        self.runtime = load_module(
            ENTITIES["city"] / "job" / "runtime.py",
            "order_completion_city_runtime_under_test",
        )

    def test_accepts_supported_interval_formats(self):
        for value in (
            "2026-08-06T00:00:00",
            "2026-08-06T00:00:00+00:00",
            "2026-08-06T00:00:00Z",
            "2026-08-06 00:00:00+00:00",
            "2026-08-06 00:00:00",
            "2026-08-06",
        ):
            with self.subTest(value=value):
                self.assertEqual(
                    self.runtime.previous_utc_date(value),
                    date(2026, 8, 5),
                )

    def test_converts_non_utc_offsets_before_shifting(self):
        # 2026-08-06 02:00+05:00 == 2026-08-05 21:00 UTC -> partition 2026-08-04.
        self.assertEqual(
            self.runtime.previous_utc_date("2026-08-06T02:00:00+05:00"),
            date(2026, 8, 4),
        )

    def test_rejects_unsupported_value_with_diagnostic(self):
        with self.assertRaises(ValueError) as raised:
            self.runtime.previous_utc_date("06/08/2026")
        self.assertIn("06/08/2026", str(raised.exception))


class OrderCompletionRuntimeContractTest(unittest.TestCase):
    def test_identifier_is_schema_and_table_only(self):
        for name, entity_dir in ENTITIES.items():
            with self.subTest(entity=name):
                runtime = load_module(
                    entity_dir / "job" / "runtime.py",
                    f"order_completion_{name}_runtime_identifier",
                )
                config = runtime.load_config(entity_dir / "config.yaml")
                ref = runtime.table_ref(config)
                catalog = FakeCatalog()
                runtime.preflight_table(catalog, ref)
                self.assertEqual(
                    catalog.identifiers,
                    [
                        ("exists", (config["table"]["schema"], config["table"]["name"])),
                        ("load", (config["table"]["schema"], config["table"]["name"])),
                    ],
                )

    def test_malformed_identifiers_are_rejected(self):
        runtime = load_module(
            ENTITIES["city"] / "job" / "runtime.py",
            "order_completion_city_runtime_malformed",
        )
        for table in (
            {"catalog": "iceberg", "schema": "silver.table", "name": "table"},
            {"catalog": "iceberg", "schema": "silver", "name": "silver.table"},
            {"catalog": "iceberg", "schema": "", "name": "table"},
        ):
            with self.subTest(table=table):
                with self.assertRaises(ValueError):
                    runtime.table_ref({"table": table})

    def test_missing_table_reports_migration_contract(self):
        runtime = load_module(
            ENTITIES["city"] / "job" / "runtime.py",
            "order_completion_city_runtime_missing",
        )
        config = runtime.load_config(ENTITIES["city"] / "config.yaml")
        ref = runtime.table_ref(config)
        with self.assertRaises(RuntimeError) as raised:
            runtime.preflight_table(FakeCatalog(exists=False), ref)
        self.assertIn("migrations", str(raised.exception))

    def test_lookback_days_comes_from_config(self):
        for name, entity_dir in ENTITIES.items():
            with self.subTest(entity=name):
                runtime = load_module(
                    entity_dir / "job" / "runtime.py",
                    f"order_completion_{name}_runtime_lookback",
                )
                config = runtime.load_config(entity_dir / "config.yaml")
                self.assertEqual(runtime.lookback_days(config), 120)
                with self.assertRaises(ValueError):
                    runtime.lookback_days({"source": {"lookback_days": "0"}})


class OrderCompletionQueryTest(unittest.TestCase):
    def build(self, name: str) -> str:
        query = load_module(
            ENTITIES[name] / "job" / "query.py",
            f"order_completion_{name}_query_under_test",
        )
        return query.build_query(date(2026, 8, 1), 120)

    def test_every_query_pins_snapshot_and_window(self):
        for name in ENTITIES:
            with self.subTest(entity=name):
                sql = self.build(name)
                self.assertIn("analyze_date = DATE '2026-08-01'", sql)
                self.assertIn("INTERVAL '120' DAY", sql)
                self.assertIn("DATE '2026-08-01' AS date", sql)
                self.assertIn("WHERE total_orders > 0", sql)
                self.assertIn(
                    "COUNT(DISTINCT order_id) FILTER (\n            "
                    "WHERE real_order_item_status != 'ACTIVE'\n        ) AS total_orders",
                    sql,
                )
                for cause in EXCLUDED_RETURN_CAUSES:
                    self.assertIn(f"'{cause}'", sql)

    def test_source_tables_stay_visible(self):
        self.assertIn('"dwh-iceberg".silver.history_order_items', self.build("city"))
        self.assertIn('"dwh-iceberg".silver.history_order_items', self.build("region"))
        category_sql = self.build("category")
        self.assertIn('"dwh-iceberg".silver.history_order_items', category_sql)
        self.assertIn('"dwh-clickhouse".dict.category', category_sql)

    def test_category_query_emits_all_five_levels_strictly(self):
        sql = self.build("category")
        for level, column in (
            ("leaf", "leaf_category"),
            ("l1", "l1_category"),
            ("l2", "l2_category"),
            ("l3", "l3_category"),
            ("l4", "l4_category"),
        ):
            self.assertIn(f"ROW('{level}', {column})", sql)
        # Строгий фильтр уровня: fallback на ближайшего предка не применяется.
        self.assertIn("AND levels.category_id > 0", sql)
        self.assertNotIn("COALESCE", sql.upper())


class OrderCompletionOrchestrationTest(unittest.TestCase):
    def test_group_shares_one_tag_and_daily_utc_schedule(self):
        for name, entity_dir in ENTITIES.items():
            with self.subTest(entity=name):
                config = yaml.safe_load(
                    (entity_dir / "config.yaml").read_text(encoding="utf-8")
                )
                self.assertEqual(config["dag"]["group_tag"], GROUP_TAG)
                self.assertEqual(config["dag"]["schedule"], "0 3 * * *")
                # Кавычки должны сниматься yaml, иначе pendulum.parse в dag.py упадет.
                start_date = config["dag"]["start_date"]
                self.assertEqual(start_date, "2026-04-01T00:00:00Z")
                self.assertEqual(
                    datetime.fromisoformat(start_date.replace("Z", "+00:00")),
                    datetime(2026, 4, 1, tzinfo=timezone.utc),
                )
                self.assertEqual(config["source"]["engine"], "trino")
                self.assertEqual(config["source"]["trino_conn_id"], "trino_search")
                self.assertEqual(config["table"]["catalog"], "iceberg")
                self.assertEqual(config["table"]["schema"], "silver")

                # DAG id живет в config.yaml; dag.py читает его через CONFIG.
                self.assertIn(
                    config["dag"]["id"],
                    (entity_dir / "config.yaml").read_text(encoding="utf-8"),
                )
                self.assertEqual(
                    config["dag"]["id"],
                    "feature-platform.layers.silver."
                    f"{entity_dir.parents[1].name}.{entity_dir.parent.name}",
                )

                dag_source = (entity_dir / "dag.py").read_text(encoding="utf-8")
                self.assertIn('CONFIG["dag"]["id"]', dag_source)
                self.assertIn('timezone="UTC"', dag_source)
                self.assertIn("require_non_empty", dag_source)
                self.assertIn("previous_utc_date", dag_source)

    def test_primary_key_matches_migration_columns(self):
        for name, entity_dir in ENTITIES.items():
            with self.subTest(entity=name):
                config = yaml.safe_load(
                    (entity_dir / "config.yaml").read_text(encoding="utf-8")
                )
                create_sql = (
                    entity_dir / "migrations" / "create_table.sql"
                ).read_text(encoding="utf-8")
                for column in config["table"]["primary_key"].split(","):
                    self.assertIn(f"    {column.strip()} ", create_sql)
                self.assertIn("part_completed_orders DOUBLE", create_sql)
                self.assertIn("part_no_show_from_total DOUBLE", create_sql)


if __name__ == "__main__":
    unittest.main()
