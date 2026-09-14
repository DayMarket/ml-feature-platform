import ast
import importlib.util
import re
import sys
import types
import unittest
from datetime import timedelta
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
GROUP_DIR = ROOT / "layers" / "gold" / "category_id_query_text"
ENTITY_DIR = GROUP_DIR / "query_category_relevance_expanded" / "v1"
SOURCE_DIR = GROUP_DIR / "query_category_relevance" / "v1"
QUERY_ID_DIR = ROOT / "layers" / "gold" / "query_text_version" / "search_query_id" / "v1"

DAG_ID = "feature-platform.layers.gold.category_id_query_text.query_category_relevance_expanded"
SOURCE_DAG_ID = "feature-platform.layers.gold.category_id_query_text.query_category_relevance"
QUERY_ID_DAG_ID = "feature-platform.layers.gold.query_text_version.search_query_id"

COLUMN_PATTERN = re.compile(r"^\s{4}([A-Za-z_][A-Za-z0-9_]*)\s+[A-Z]", re.MULTILINE)


def load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def migration_columns(entity_dir: Path) -> list[str]:
    sql = (entity_dir / "migrations" / "create_table.sql").read_text(encoding="utf-8")
    return COLUMN_PATTERN.findall(sql)


def cron_minutes_of_day(cron: str) -> int:
    minute, hour = cron.split()[:2]
    return int(hour) * 60 + int(minute)


def load_job_module():
    # Заглушки pyspark и пакет job ставятся только на время импорта: оставленные в
    # sys.modules, они подменили бы полноценные заглушки других тестов (test_query_id_features).
    job_name = "job.getting_query_category_relevance_expanded"
    touched = ("pyspark", "pyspark.sql", "job", "job.entities", job_name)
    saved = {name: sys.modules.get(name) for name in touched}
    try:
        pyspark_module = types.ModuleType("pyspark")
        pyspark_sql_module = types.ModuleType("pyspark.sql")
        pyspark_sql_module.SparkSession = object
        pyspark_module.sql = pyspark_sql_module
        sys.modules["pyspark"] = pyspark_module
        sys.modules["pyspark.sql"] = pyspark_sql_module
        job_package = types.ModuleType("job")
        job_package.__path__ = [str(ENTITY_DIR / "job")]
        sys.modules["job"] = job_package
        for name in ("entities", "getting_query_category_relevance_expanded"):
            spec = importlib.util.spec_from_file_location(
                f"job.{name}", ENTITY_DIR / "job" / f"{name}.py"
            )
            module = importlib.util.module_from_spec(spec)
            sys.modules[f"job.{name}"] = module
            spec.loader.exec_module(module)
        return sys.modules[job_name]
    finally:
        for name, module in saved.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


def _evaluate(node: ast.AST, constants: dict):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return constants[node.id]
    if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "timedelta":
        return timedelta(**{kw.arg: _evaluate(kw.value, constants) for kw in node.keywords})
    raise AssertionError(f"не вычисляется статически: {ast.dump(node)}")


def dag_sensors(dag_path: Path) -> dict[str, dict]:
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))
    constants = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                try:
                    constants[target.id] = _evaluate(node.value, constants)
                except (AssertionError, KeyError):
                    pass
    sensors = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ExternalTaskSensor":
            keywords = {kw.arg: kw.value for kw in node.keywords}
            sensors[_evaluate(keywords["external_dag_id"], constants)] = {
                name: _evaluate(keywords[name], constants)
                for name in ("external_task_id", "execution_delta")
            }
    return sensors


class ExpandedConfigAndMigrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_yaml(ENTITY_DIR / "config.yaml")

    def test_table_identity(self):
        table = self.config["table"]
        self.assertEqual(table["catalog"], "iceberg")
        self.assertEqual(table["schema"], "gold")
        self.assertEqual(table["name"], "feature_platform_query_category_relevance_expanded")
        self.assertEqual(table["primary_key"], "date,category_id,query_text")
        self.assertEqual(table["meta"]["team"], "team:search")

    def test_orchestration_and_alerts(self):
        self.assertEqual(self.config["dag"]["id"], DAG_ID)
        self.assertEqual(self.config["dag"]["group_tag"], "query-category-relevance")
        self.assertEqual(self.config["dag"]["schedule"], "30 3 * * *")
        self.assertEqual(
            self.config["alerts"],
            {"team": "search", "severity": "P3", "oncall_webhook_conn_id": "oncall_webhook_search"},
        )

    def test_spark_application_points_at_this_entity(self):
        spark = self.config["spark"]
        self.assertEqual(spark["resource_profile"], "medium")
        self.assertEqual(
            spark["main_application_file"],
            "local:///git/repo/layers/gold/category_id_query_text/"
            "query_category_relevance_expanded/v1/entrypoints/"
            "get_query_category_relevance_expanded.py",
        )
        self.assertTrue((ENTITY_DIR / "entrypoints" / "get_query_category_relevance_expanded.py").is_file())

    def test_dq_and_feature_stats_look_at_the_same_partition(self):
        self.assertEqual(
            self.config["dq"]["partition_date_template"],
            self.config["feature_stats"]["partition_date_template"],
        )

    def test_migration_columns(self):
        self.assertEqual(
            migration_columns(ENTITY_DIR),
            ["date", "category_id", "query_text", "relevance"],
        )
        sql = (ENTITY_DIR / "migrations" / "create_table.sql").read_text(encoding="utf-8")
        self.assertIn("PARTITIONED BY (date)", sql)
        self.assertIn("'engine.hive.lock-enabled' = 'false'", sql)


class ExpandedJobTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module()

    def test_supported_partition_date_formats(self):
        for value in (
            "2026-06-17 00:00:00",
            "2026-06-17T00:00:00",
            "2026-06-17T00:00:00+00:00",
            "2026-06-17T00:00:00Z",
            "2026-06-17 00:00:00+00:00",
        ):
            with self.subTest(value=value):
                self.assertEqual(self.job.parse_partition_date(value), "2026-06-17")

    def test_unsupported_partition_date_raises_with_value(self):
        with self.assertRaises(ValueError) as error:
            self.job.parse_partition_date("17.06.2026")
        self.assertIn("17.06.2026", str(error.exception))

    def test_source_tables_are_explicit(self):
        self.assertEqual(self.job.SOURCE_TABLE, "iceberg.gold.feature_platform_query_category_relevance")
        self.assertEqual(self.job.QUERY_ID_TABLE, "iceberg.gold.feature_platform_search_query_id")

    def test_selected_columns_match_migration(self):
        self.assertEqual(list(self.job.SELECTED_COLUMNS), migration_columns(ENTITY_DIR))

    def test_query_merges_dictionary_lowercases_and_keeps_one_row_per_pair(self):
        query = " ".join(self.job.render_query("2026-09-14").split())

        self.assertIn("WHERE date <= DATE '2026-09-14'", query)
        self.assertIn("UNION ALL", query)
        self.assertIn(
            f"JOIN {self.job.QUERY_ID_TABLE} AS dictionary ON dictionary.query_id = source.query_id",
            query,
        )
        self.assertIn("lower(query_text) AS query_text", query)
        self.assertIn(
            "row_number() OVER ( PARTITION BY category_id, lower(query_text) "
            "ORDER BY date DESC, relevance DESC NULLS LAST )",
            query,
        )
        self.assertIn("DATE '2026-09-14' AS date", query)
        # Кроме lower текст запроса ничем не нормализуется.
        for forbidden in ("trim(", "regexp_replace(", "translate("):
            self.assertNotIn(forbidden, query.lower())

    def test_query_rejects_non_date_run_date(self):
        with self.assertRaises(ValueError):
            self.job.render_query("2026-09-14' OR 1=1 --")


class ExpandedDagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dag_path = ENTITY_DIR / "dag.py"
        cls.source = cls.dag_path.read_text(encoding="utf-8")
        cls.sensors = dag_sensors(cls.dag_path)
        cls.schedule = load_yaml(ENTITY_DIR / "config.yaml")["dag"]["schedule"]

    def test_waits_for_source_mart_materialize_on_the_same_day(self):
        sensor = self.sensors[SOURCE_DAG_ID]
        source_schedule = load_yaml(SOURCE_DIR / "config.yaml")["dag"]["schedule"]
        self.assertEqual(sensor["external_task_id"], "materialize")
        self.assertEqual(
            sensor["execution_delta"],
            timedelta(minutes=cron_minutes_of_day(self.schedule) - cron_minutes_of_day(source_schedule)),
        )

    def test_waits_for_previous_day_query_id_dictionary_dq(self):
        sensor = self.sensors[QUERY_ID_DAG_ID]
        query_id_schedule = load_yaml(QUERY_ID_DIR / "config.yaml")["dag"]["schedule"]
        self.assertEqual(sensor["external_task_id"], "dq")
        # Справочник стартует в 05:00, позже этого DAG'а: ждём прогон предыдущей даты.
        self.assertEqual(
            sensor["execution_delta"],
            timedelta(
                minutes=24 * 60
                + cron_minutes_of_day(self.schedule)
                - cron_minutes_of_day(query_id_schedule)
            ),
        )

    def test_builds_dq_and_feature_stats_in_parallel(self):
        self.assertIn("build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)", self.source)
        self.assertIn("build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)", self.source)
        self.assertIn(">> [dq_task, stats_task]", self.source)

    def test_dag_is_paused_upon_creation(self):
        self.assertIn("is_paused_upon_creation=True", self.source)
        self.assertIn("catchup=False", self.source)


if __name__ == "__main__":
    unittest.main()
