import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

QUERY_ORIGIN = ROOT / "layers" / "gold" / "query" / "search_query_atc_features" / "v1"
QUERY_QID = ROOT / "layers" / "gold" / "query" / "search_query_atc_features_qid" / "v1"

PAIR_ORIGIN = (
    ROOT / "layers" / "gold" / "query_sku_group_id"
    / "sku_group_query_atc_order_features" / "v2"
)
PAIR_QID = (
    ROOT / "layers" / "gold" / "query_sku_group_id"
    / "sku_group_query_atc_order_features_qid" / "v1"
)

SERVICE_COLUMNS = ("query_id", "has_query_id")

COLUMN_PATTERN = re.compile(r"^\s{4}([A-Za-z_][A-Za-z0-9_]*)\s+[A-Z]", re.MULTILINE)

DAG_ID_PATTERN = re.compile(r'dag_id="([^"]+)"')
CRON_PATTERN = re.compile(r"CronDataIntervalTimetable\(\s*['\"]([^'\"]+)['\"]")


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


import importlib.util
import sys
import types


def stub_pyspark() -> None:
    """Подменяет pyspark заглушками: локально его нет, а нам нужны только чистые функции."""
    if "pyspark" in sys.modules:
        return
    pyspark_module = types.ModuleType("pyspark")
    pyspark_sql_module = types.ModuleType("pyspark.sql")
    pyspark_sql_module.DataFrame = object
    pyspark_sql_module.SparkSession = object
    pyspark_sql_functions_module = types.ModuleType("pyspark.sql.functions")
    pyspark_sql_column_module = types.ModuleType("pyspark.sql.column")
    pyspark_sql_column_module.Column = object
    pyspark_sql_module.functions = pyspark_sql_functions_module
    pyspark_sql_module.column = pyspark_sql_column_module
    pyspark_module.sql = pyspark_sql_module
    sys.modules["pyspark"] = pyspark_module
    sys.modules["pyspark.sql"] = pyspark_sql_module
    sys.modules["pyspark.sql.functions"] = pyspark_sql_functions_module
    sys.modules["pyspark.sql.column"] = pyspark_sql_column_module


def load_job_module(entity_dir: Path, filename: str, module_name: str):
    stub_pyspark()
    entities_spec = importlib.util.spec_from_file_location(
        "job.entities",
        entity_dir / "job" / "entities.py",
    )
    entities_module = importlib.util.module_from_spec(entities_spec)
    job_package = types.ModuleType("job")
    job_package.__path__ = [str(entity_dir / "job")]
    sys.modules["job"] = job_package
    sys.modules["job.entities"] = entities_module
    entities_spec.loader.exec_module(entities_module)

    spec = importlib.util.spec_from_file_location(
        module_name,
        entity_dir / "job" / filename,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class QueryLevelJobTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module(
            QUERY_QID,
            "getting_search_query_atc_features_qid.py",
            "test_query_atc_features_qid_job",
        )

    def test_normalization_lowercases_folds_yo_and_collapses_spaces(self):
        self.assertEqual(self.job.normalize_query_value("  Красные   КРОССОВКИ "), "красные кроссовки")
        self.assertEqual(self.job.normalize_query_value("ЁЛКА"), "елка")
        self.assertEqual(self.job.normalize_query_value("ёлка"), "елка")

    def test_normalization_of_blank_input_is_empty(self):
        for value in (None, "", "   ", "\t\n"):
            with self.subTest(value=value):
                self.assertEqual(self.job.normalize_query_value(value), "")

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

    def test_dictionary_source_is_pinned_to_v1(self):
        self.assertEqual(
            self.job.QUERY_ID_TABLE,
            "iceberg.gold.feature_platform_search_query_id",
        )
        self.assertEqual(self.job.QUERY_ID_VERSION, "v1")

    def test_selected_columns_match_migration_columns(self):
        self.assertEqual(list(self.job.SELECTED_COLUMNS), migration_columns(QUERY_QID))

    def test_windows_mirror_the_origin_job(self):
        origin_source = (
            QUERY_ORIGIN / "job" / "getting_search_query_atc_features.py"
        ).read_text(encoding="utf-8")

        self.assertIn("WINDOWS = (1, 3, 7, 14, 21, 30, 60, 90)", origin_source)
        self.assertEqual(self.job.WINDOWS, (1, 3, 7, 14, 21, 30, 60, 90))


class QueryLevelDagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (QUERY_QID / "dag.py").read_text(encoding="utf-8")

    def test_dag_id_encodes_repository_path(self):
        self.assertEqual(
            DAG_ID_PATTERN.search(self.source).group(1),
            "feature-platform.layers.gold.query.search_query_atc_features_qid",
        )

    def test_schedule_runs_after_the_query_id_dag(self):
        self.assertEqual(CRON_PATTERN.search(self.source).group(1), "0 6 * * *")

    def test_it_waits_for_the_query_id_dag_itself_not_its_dq(self):
        self.assertIn(
            '"feature-platform.layers.gold.query_text_version.search_query_id"',
            self.source,
        )
        self.assertNotIn(
            "ml_feature_platform_gold.feature_platform_search_query_id.dq",
            self.source,
        )

    def test_it_waits_for_both_silver_dq_dags(self):
        self.assertIn(
            "feature_platform_search_sku_group_id_install_query.dq",
            self.source,
        )
        self.assertIn(
            "feature_platform_sku_group_query_search_orders.dq",
            self.source,
        )

    def test_execution_deltas_line_up_with_the_schedule(self):
        # 06:00 логической даты минус 1 час = 05:00, логическая дата прогона search_query_id.
        # 06:00 минус 5 часов = 01:00, логическая дата DQ силверов.
        self.assertIn("execution_delta=timedelta(hours=1)", self.source)
        self.assertIn("execution_delta=timedelta(hours=5)", self.source)

    def test_dag_is_paused_upon_creation(self):
        self.assertIn("is_paused_upon_creation=True", self.source)


class PairMigrationAndConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = read_simple_config(PAIR_QID / "config.yaml")

    def test_feature_columns_mirror_the_origin_table(self):
        origin = migration_columns(PAIR_ORIGIN)
        qid = migration_columns(PAIR_QID)

        self.assertEqual(
            [column for column in qid if column not in SERVICE_COLUMNS],
            origin,
        )

    def test_service_columns_are_present_right_after_the_key(self):
        columns = migration_columns(PAIR_QID)

        self.assertEqual(
            columns[:5],
            ["date", "query", "sku_group_id", "query_id", "has_query_id"],
        )

    def test_hive_locks_are_disabled(self):
        sql = (PAIR_QID / "migrations" / "create_table.sql").read_text(encoding="utf-8")

        self.assertIn("'engine.hive.lock-enabled' = 'false'", sql)
        self.assertIn("PARTITIONED BY (date)", sql)

    def test_table_identity(self):
        table = self.config["table"]

        self.assertEqual(table["catalog"], "iceberg")
        self.assertEqual(table["schema"], "gold")
        self.assertEqual(
            table["name"],
            "feature_platform_search_sku_group_id_query_atc_order_features_qid",
        )
        self.assertEqual(table["primary_key"], "date,query,sku_group_id")
        self.assertEqual(table["meta"]["team"], "team:search")

    def test_primary_key_matches_layer_directory_group(self):
        self.assertEqual(PAIR_QID.parents[1].name, "query_sku_group_id")

    def test_resource_profile_matches_the_origin(self):
        origin_config = read_simple_config(PAIR_ORIGIN / "config.yaml")

        self.assertEqual(
            self.config["spark"]["resource_profile"],
            origin_config["spark"]["resource_profile"],
        )


class PairJobTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.job = load_job_module(
            PAIR_QID,
            "getting_sku_group_query_atc_order_features_qid.py",
            "test_sku_group_query_atc_order_features_qid_job",
        )

    def test_selected_columns_match_migration_columns(self):
        self.assertEqual(list(self.job.SELECTED_COLUMNS), migration_columns(PAIR_QID))

    def test_smoothing_coefficient_mirrors_the_origin_job(self):
        origin_source = (
            PAIR_ORIGIN / "job" / "getting_sku_group_query_atc_order_features.py"
        ).read_text(encoding="utf-8")

        self.assertIn("SMOOTHING_COEF = 100.0", origin_source)
        self.assertEqual(self.job.SMOOTHING_COEF, 100.0)

    def test_windows_mirror_the_origin_job(self):
        self.assertEqual(self.job.WINDOWS, (1, 3, 7, 14, 21, 30, 60, 90))

    def test_normalization_is_identical_to_the_query_level_job(self):
        query_job = load_job_module(
            QUERY_QID,
            "getting_search_query_atc_features_qid.py",
            "test_query_atc_features_qid_job_for_pair",
        )

        self.assertEqual(
            self.job.NORMALIZATION_REPLACEMENTS,
            query_job.NORMALIZATION_REPLACEMENTS,
        )
        for value in ("  Ёлка   ЗЕЛЁНАЯ ", "krossovka", ""):
            with self.subTest(value=value):
                self.assertEqual(
                    self.job.normalize_query_value(value),
                    query_job.normalize_query_value(value),
                )

    def test_dictionary_source_is_pinned_to_v1(self):
        self.assertEqual(
            self.job.QUERY_ID_TABLE,
            "iceberg.gold.feature_platform_search_query_id",
        )
        self.assertEqual(self.job.QUERY_ID_VERSION, "v1")

    def test_partition_date_is_parsed_not_sliced(self):
        source = (
            PAIR_QID / "job" / "getting_sku_group_query_atc_order_features_qid.py"
        ).read_text(encoding="utf-8")

        self.assertNotIn("partition_start[:10]", source)
        self.assertEqual(self.job.parse_partition_date("2026-06-17T00:00:00Z"), "2026-06-17")

    def test_sku_level_denominators_stay_grouped_by_sku_group_id_only(self):
        source = (
            PAIR_QID / "job" / "getting_sku_group_query_atc_order_features_qid.py"
        ).read_text(encoding="utf-8")

        self.assertIn('daily_events.groupBy("sku_group_id")', source)
        self.assertIn('orders.groupBy("sku_group_id")', source)

    def test_pair_aggregations_are_grouped_by_group_key(self):
        source = (
            PAIR_QID / "job" / "getting_sku_group_query_atc_order_features_qid.py"
        ).read_text(encoding="utf-8")

        self.assertIn('groupBy("group_key", "sku_group_id")', source)
        self.assertNotIn('groupBy("query", "sku_group_id")', source)

    def test_pair_joins_key_on_group_key_not_query(self):
        source = (
            PAIR_QID / "job" / "getting_sku_group_query_atc_order_features_qid.py"
        ).read_text(encoding="utf-8")

        self.assertIn('on=["group_key", "sku_group_id"]', source)
        self.assertNotIn('on=["query", "sku_group_id"]', source)

    def test_null_fill_excludes_group_key_not_query(self):
        source = (
            PAIR_QID / "job" / "getting_sku_group_query_atc_order_features_qid.py"
        ).read_text(encoding="utf-8")

        self.assertIn(
            'if column_name not in ("group_key", "sku_group_id"):',
            source,
        )
        self.assertNotIn(
            'if column_name not in ("query", "sku_group_id"):',
            source,
        )

    def test_date_is_set_after_the_expansion_join(self):
        source = (
            PAIR_QID / "job" / "getting_sku_group_query_atc_order_features_qid.py"
        ).read_text(encoding="utf-8")

        self.assertLess(
            source.index('members.join('),
            source.index('withColumn("date"'),
        )

    def test_features_are_expanded_from_group_key_back_to_query(self):
        source = (
            PAIR_QID / "job" / "getting_sku_group_query_atc_order_features_qid.py"
        ).read_text(encoding="utf-8")

        self.assertIn(
            'members.join(features, on="group_key", how="inner")',
            source,
        )
        self.assertIn('.withColumnRenamed("group_key", "query_id")', source)


if __name__ == "__main__":
    unittest.main()
