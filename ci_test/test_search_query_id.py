import importlib.util
import sys
import unittest
from datetime import date, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENTITY_DIR = (
    ROOT / "layers" / "gold" / "query_text_version" / "search_query_id" / "v1"
)


def load_job_module(filename: str, module_name: str):
    spec = importlib.util.spec_from_file_location(
        module_name,
        ENTITY_DIR / "job" / filename,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def analyzer_tokens(*positions):
    tokens = []
    for position, variants in enumerate(positions):
        for variant in variants:
            tokens.append({"token": variant, "position": position})
    return tokens


class SearchQueryIdNormalizeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.normalize = load_job_module("normalize.py", "test_search_query_id_normalize")

    def test_stop_words_are_removed_on_word_boundaries(self):
        pattern = self.normalize.build_stop_words_pattern(["для", "и"])

        self.assertEqual(
            self.normalize.remove_stop_words("Красные КРОССОВКИ для бега", pattern),
            "красные кроссовки бега",
        )

    def test_stop_word_inside_another_word_is_kept(self):
        pattern = self.normalize.build_stop_words_pattern(["для"])

        self.assertEqual(
            self.normalize.remove_stop_words("подля", pattern),
            "подля",
        )

    def test_multi_word_stop_phrase_wins_over_its_first_word(self):
        pattern = self.normalize.build_stop_words_pattern(
            ["aksiya", "aksiya tavarlar", "eng", "eng arzon", "eng arzon narsalar"]
        )

        self.assertEqual(
            self.normalize.remove_stop_words("aksiya tavarlar krossovka", pattern),
            "krossovka",
        )
        self.assertEqual(
            self.normalize.remove_stop_words("eng arzon narsalar telefon", pattern),
            "telefon",
        )

    def test_no_stop_phrase_is_shadowed_by_a_shorter_entry(self):
        words = self.normalize.load_stop_words(ENTITY_DIR / "job" / "stop_words.txt")
        pattern = self.normalize.build_stop_words_pattern(words)

        for phrase in (word for word in words if " " in word):
            with self.subTest(phrase=phrase):
                self.assertEqual(
                    self.normalize.remove_stop_words(f"{phrase} krossovka", pattern),
                    "krossovka",
                )

    def test_empty_stop_word_list_only_normalizes_spacing(self):
        pattern = self.normalize.build_stop_words_pattern([])

        self.assertIsNone(pattern)
        self.assertEqual(
            self.normalize.remove_stop_words("  Красные   КРОССОВКИ ", pattern),
            "красные кроссовки",
        )

    def test_load_stop_words_skips_comments_and_blank_lines(self):
        self.assertEqual(
            self.normalize.load_stop_words(ENTITY_DIR / "job" / "stop_words.txt"),
            self.normalize.load_stop_words(ENTITY_DIR / "job" / "stop_words.txt"),
        )
        for word in self.normalize.load_stop_words(
            ENTITY_DIR / "job" / "stop_words.txt"
        ):
            self.assertFalse(word.startswith("#"))
            self.assertEqual(word, word.strip().lower())

    def test_tokens_are_grouped_and_deduplicated_by_position(self):
        tokens = analyzer_tokens(["красн", "красн"], ["кроссовк", "кед"])

        self.assertEqual(
            self.normalize.group_tokens_by_position(tokens),
            [["красн"], ["кед", "кроссовк"]],
        )

    def test_query_id_is_word_order_independent(self):
        direct = analyzer_tokens(["красн"], ["кроссовк"], ["бег"])
        reordered = analyzer_tokens(["бег"], ["кроссовк"], ["красн"])

        self.assertEqual(
            self.normalize.build_query_id(direct),
            self.normalize.build_query_id(reordered),
        )
        self.assertEqual(self.normalize.build_query_id(direct), "бег красн кроссовк")

    def test_query_id_takes_first_variant_per_position(self):
        tokens = analyzer_tokens(["кроссовк", "кед"])

        self.assertEqual(self.normalize.build_query_id(tokens), "кед")

    def test_query_id_is_empty_without_tokens(self):
        self.assertEqual(self.normalize.build_query_id([]), "")


class SearchQueryIdQueryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.query = load_job_module("query.py", "test_search_query_id_query")

    def test_query_excludes_already_normalized_queries(self):
        sql = self.query.build_new_queries_query(
            partition_date=date(2026, 8, 6),
            install_query_table='"dwh-iceberg".silver.feature_platform_search_sku_group_id_install_query',
            query_id_table='"dwh-iceberg".gold.feature_platform_search_query_id',
            space="SEARCH_RESULTS",
            version="v1",
        )

        self.assertIn("SELECT DISTINCT install_query.uniqs AS original_query", sql)
        self.assertIn("LEFT JOIN \"dwh-iceberg\".gold.feature_platform_search_query_id", sql)
        self.assertIn("known_query.version = 'v1'", sql)
        self.assertIn("known_query.query_text IS NULL", sql)
        self.assertIn("install_query.date = DATE '2026-08-06'", sql)
        self.assertIn("install_query.space = 'SEARCH_RESULTS'", sql)

    def test_string_literals_are_escaped(self):
        sql = self.query.build_new_queries_query(
            partition_date=date(2026, 8, 6),
            install_query_table="silver.source",
            query_id_table="gold.target",
            space="SEARCH_RESULTS",
            version="v'1",
        )

        self.assertIn("known_query.version = 'v''1'", sql)


class SearchQueryIdPartitionDateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = load_job_module("runtime.py", "test_search_query_id_runtime")

    def test_supported_partition_date_formats(self):
        expected = date(2026, 6, 17)
        for value in (
            "2026-06-17",
            "2026-06-17T00:00:00",
            "2026-06-17T00:00:00+00:00",
            "2026-06-17T00:00:00Z",
            "2026-06-17 00:00:00+00:00",
            "2026-06-17 00:00:00",
        ):
            with self.subTest(value=value):
                self.assertEqual(self.runtime.parse_partition_date(value), expected)

    def test_unsupported_partition_date_raises_with_value(self):
        with self.assertRaises(ValueError) as error:
            self.runtime.parse_partition_date("17.06.2026")

        self.assertIn("17.06.2026", str(error.exception))

    def test_supported_snapshot_timestamp_formats(self):
        expected = datetime(2026, 6, 17, 5, 0, 0)
        for value in (
            "2026-06-17T05:00:00",
            "2026-06-17T05:00:00+00:00",
            "2026-06-17T05:00:00Z",
            "2026-06-17 05:00:00+00:00",
            "2026-06-17 05:00:00",
        ):
            with self.subTest(value=value):
                self.assertEqual(
                    self.runtime.parse_snapshot_timestamp(value), expected
                )

    def test_snapshot_timestamp_is_converted_to_naive_utc(self):
        self.assertEqual(
            self.runtime.parse_snapshot_timestamp("2026-06-17T08:00:00+03:00"),
            datetime(2026, 6, 17, 5, 0, 0),
        )


class SearchQueryIdIdentifierTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = load_job_module("runtime.py", "test_search_query_id_runtime")

    def test_table_ref_builds_two_part_pyiceberg_identifier(self):
        ref = self.runtime.table_ref(
            {
                "table": {
                    "catalog": "iceberg",
                    "schema": "gold",
                    "name": "feature_platform_search_query_id",
                }
            }
        )

        self.assertEqual(ref.identifier, ("gold", "feature_platform_search_query_id"))
        self.assertEqual(
            ref.qualified_name,
            "iceberg.gold.feature_platform_search_query_id",
        )
        self.assertEqual(
            self.runtime.trino_table_name(ref),
            '"dwh-iceberg".gold.feature_platform_search_query_id',
        )

    def test_malformed_identifiers_are_rejected(self):
        for table in (
            {"catalog": "iceberg", "schema": "", "name": "table"},
            {"catalog": "iceberg", "schema": "gold", "name": ""},
            {"catalog": "iceberg", "schema": "gold.feature", "name": "table"},
            {"catalog": "iceberg", "schema": "gold", "name": "schema.table"},
            {"catalog": "iceberg", "schema": "gold", "name": "catalog.schema.table"},
        ):
            with self.subTest(table=table):
                with self.assertRaises(ValueError):
                    self.runtime.table_ref({"table": table})


class SearchQueryIdConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = load_job_module("runtime.py", "test_search_query_id_runtime")
        cls.config = cls.runtime.load_config(ENTITY_DIR / "config.yaml")

    def test_output_version_matches_entity_version_directory(self):
        self.assertEqual(self.config["output"]["version"], "v1")

    def test_primary_key_matches_layer_directory_group(self):
        self.assertEqual(self.config["table"]["primary_key"], "query_text,version")
        self.assertEqual(ENTITY_DIR.parents[1].name, "query_text_version")

    def test_dag_id_encodes_repository_path(self):
        self.assertEqual(
            self.config["dag"]["id"],
            "feature-platform.layers.gold.query_text_version.search_query_id",
        )

    def test_migration_columns_match_runtime_output_columns(self):
        create_table = (ENTITY_DIR / "migrations" / "create_table.sql").read_text(
            encoding="utf-8"
        )

        for column in self.runtime.OUTPUT_COLUMNS:
            self.assertIn(f"    {column} ", create_table)


if __name__ == "__main__":
    unittest.main()
