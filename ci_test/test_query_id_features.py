import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

QUERY_ORIGIN = ROOT / "layers" / "gold" / "query" / "search_query_atc_features" / "v1"
QUERY_QID = ROOT / "layers" / "gold" / "query" / "search_query_atc_features_qid" / "v1"

SERVICE_COLUMNS = ("query_id", "has_query_id")

COLUMN_PATTERN = re.compile(r"^\s{4}([A-Za-z_][A-Za-z0-9_]*)\s+[A-Z]", re.MULTILINE)


def migration_columns(entity_dir: Path) -> list[str]:
    sql = (entity_dir / "migrations" / "create_table.sql").read_text(encoding="utf-8")
    return COLUMN_PATTERN.findall(sql)


def read_simple_config(path: Path) -> dict:
    config: dict = {}
    stack: list[tuple[int, dict]] = [(-1, config)]
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        key, separator, value = raw_line.strip().partition(":")
        if not separator or not key:
            continue
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value.strip():
            parent[key.strip()] = value.strip()
        else:
            nested: dict = {}
            parent[key.strip()] = nested
            stack.append((indent, nested))
    return config


class QueryLevelMigrationTest(unittest.TestCase):
    def test_feature_columns_mirror_the_origin_table(self):
        origin = migration_columns(QUERY_ORIGIN)
        qid = migration_columns(QUERY_QID)

        self.assertEqual(
            [column for column in qid if column not in SERVICE_COLUMNS],
            origin,
        )

    def test_service_columns_are_present_right_after_the_key(self):
        columns = migration_columns(QUERY_QID)

        self.assertEqual(columns[:4], ["date", "query", "query_id", "has_query_id"])

    def test_hive_locks_are_disabled(self):
        sql = (QUERY_QID / "migrations" / "create_table.sql").read_text(encoding="utf-8")

        self.assertIn("'engine.hive.lock-enabled' = 'false'", sql)
        self.assertIn("PARTITIONED BY (date)", sql)


class QueryLevelConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = read_simple_config(QUERY_QID / "config.yaml")

    def test_table_identity(self):
        table = self.config["table"]

        self.assertEqual(table["catalog"], "iceberg")
        self.assertEqual(table["schema"], "gold")
        self.assertEqual(table["name"], "feature_platform_search_query_atc_features_qid")
        self.assertEqual(table["primary_key"], "date,query")
        self.assertEqual(table["meta"]["team"], "team:search")

    def test_primary_key_matches_layer_directory_group(self):
        self.assertEqual(QUERY_QID.parents[1].name, "query")

    def test_main_application_file_points_at_this_entity(self):
        self.assertEqual(
            self.config["spark"]["main_application_file"],
            "local:///git/repo/layers/gold/query/search_query_atc_features_qid/v1"
            "/entrypoints/get_search_query_atc_features_qid.py",
        )


if __name__ == "__main__":
    unittest.main()
