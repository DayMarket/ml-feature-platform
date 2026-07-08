import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENTITY_DIR = (
    ROOT
    / "layers"
    / "silver"
    / "query_sku_group_id"
    / "search_query_sku_group_es_features"
    / "v1"
)


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class SearchQuerySkuGroupEsFeaturesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.analyze = load_module(ENTITY_DIR / "job" / "analyze.py", "test_search_es_analyze")
        cls.runtime = load_module(ENTITY_DIR / "job" / "runtime.py", "test_search_es_runtime")
        cls.query = load_module(ENTITY_DIR / "job" / "query.py", "test_search_es_query")
        cls.search = load_module(ENTITY_DIR / "job" / "search.py", "test_search_es_search")

    def test_partition_timestamp_formats(self):
        values = [
            "2026-06-17T00:00:00",
            "2026-06-17T00:00:00+00:00",
            "2026-06-17T00:00:00Z",
            "2026-06-17 00:00:00+00:00",
            "2026-06-17 00:00:00",
        ]
        for value in values:
            with self.subTest(value=value):
                self.assertEqual(
                    self.runtime.previous_utc_date(value),
                    date(2026, 6, 16),
                )

    def test_explain_parser_keeps_root_total_score_and_field_bm25(self):
        explanation = {
            "value": 12.5,
            "description": "sum of:",
            "details": [
                {
                    "value": 3.0,
                    "description": "weight(skus.title:bandana in 106917) [PerFieldSimilarity], result of:",
                    "details": [
                        {
                            "value": 3.0,
                            "description": "score(freq=1.0), computed as boost * idf * tf from:",
                            "details": [
                                {"value": 2.0, "description": "boost", "details": []},
                                {
                                    "value": 1.5,
                                    "description": "idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:",
                                    "details": [
                                        {
                                            "value": 10,
                                            "description": "n, number of documents containing term",
                                            "details": [],
                                        },
                                        {
                                            "value": 100,
                                            "description": "N, total number of documents with field",
                                            "details": [],
                                        },
                                    ],
                                },
                                {
                                    "value": 0.5,
                                    "description": "tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from:",
                                    "details": [
                                        {
                                            "value": 1.0,
                                            "description": "freq, occurrences of term within document",
                                            "details": [],
                                        },
                                        {
                                            "value": 1.2,
                                            "description": "k1, term saturation parameter",
                                            "details": [],
                                        },
                                        {
                                            "value": 0.5,
                                            "description": "b, length normalization parameter",
                                            "details": [],
                                        },
                                        {
                                            "value": 3.0,
                                            "description": "dl, length of field",
                                            "details": [],
                                        },
                                        {
                                            "value": 4.0,
                                            "description": "avgdl, average length of field",
                                            "details": [],
                                        },
                                    ],
                                },
                            ],
                        }
                    ],
                },
                {
                    "value": 4.9,
                    "description": "field value function: none(doc['sku_group.rating'].value * factor=1.0)",
                    "details": [],
                },
            ],
        }

        analysis = self.analyze.analyze_explain(explanation)

        self.assertEqual(analysis["total_score"], 12.5)
        self.assertEqual(analysis["simple_grouping"]["total_bm25"], 3.0)
        self.assertEqual(
            analysis["simple_grouping"]["scores"]["skus.title:bandana"]["field"],
            "skus.title",
        )
        self.assertEqual(
            analysis["field_factors"]["sku_group.rating"]["value"],
            4.9,
        )

    def test_explain_parser_keeps_raw_terms_without_manual_synonym_mapping(self):
        explanation = {
            "value": 2.0,
            "description": "sum of:",
            "details": [
                {
                    "value": 2.0,
                    "description": "weight(skus.title:углевой in 1) [PerFieldSimilarity], result of:",
                    "details": [
                        {
                            "value": 2.0,
                            "description": "score(freq=1.0), computed as boost * idf * tf from:",
                            "details": [],
                        }
                    ],
                }
            ],
        }

        analysis = self.analyze.analyze_explain(explanation)

        self.assertIn("skus.title:углевой", analysis["simple_grouping"]["scores"])
        self.assertNotIn("skus.title:уголь", analysis["simple_grouping"]["scores"])

    def test_hit_to_row_creates_separate_bm25_arrays(self):
        hit = {
            "_source": {
                "sku_group": {
                    "id": 948376,
                    "price": {"sell": 1000},
                    "rating": 4.9,
                    "orders_quantity": 3,
                },
                "product": {
                    "id": 123,
                    "title": {"ru": "Bandana"},
                    "orders_quantity": 35,
                    "rating": 4.8,
                },
                "query_encoder_v3": [0.1, 0.2],
            },
            "_explanation": {
                "value": 7.0,
                "description": "sum of:",
                "details": [
                    {
                        "value": 2.0,
                        "description": "weight(skus.title:bandana in 1) [PerFieldSimilarity], result of:",
                        "details": [
                            {
                                "value": 2.0,
                                "description": "score(freq=1.0), computed as boost * idf * tf from:",
                                "details": [],
                            }
                        ],
                    }
                ],
            },
        }

        row = self.analyze.hit_to_row(
            hit,
            query="bandana",
            partition_date=date(2026, 3, 13),
            fields=["skus.title", "product.title.ru.synonym"],
        )

        self.assertEqual(row["date"], date(2026, 3, 13))
        self.assertEqual(row["query"], "bandana")
        self.assertEqual(row["sku_group_id"], 948376)
        self.assertEqual(row["bm25_skus_title"], [2.0])
        self.assertEqual(row["bm25_product_title_ru_synonym"], [])
        self.assertEqual(row["total_score"], 7.0)

    def test_query_uses_clickstream_sessions_for_partition_date(self):
        sql = self.query.build_query(
            partition_date=date(2026, 3, 13),
            clickstream_events_table='"dwh-iceberg".silver_b2c_clickstream.events',
            search_logs_table='"dwh-iceberg".silver.search_logs',
        )

        self.assertIn('FROM "dwh-iceberg".silver_b2c_clickstream.events', sql)
        self.assertIn("event_type = 'PRODUCT_IMPRESSION'", sql)
        self.assertIn("widget_space_name = 'SEARCH_RESULTS'", sql)
        self.assertIn("received_at >= CAST('2026-03-13' AS TIMESTAMP(6))", sql)
        self.assertIn("received_at < CAST('2026-03-14' AS TIMESTAMP(6))", sql)
        self.assertIn("logged_at >= CAST('2026-03-10' AS TIMESTAMP(6))", sql)
        self.assertIn("logged_at < CAST('2026-03-17' AS TIMESTAMP(6))", sql)
        self.assertIn("logged_at >= CAST('2026-03-12' AS TIMESTAMP(6))", sql)
        self.assertIn("logged_at < CAST('2026-03-14' AS TIMESTAMP(6))", sql)
        self.assertIn("array_agg(DISTINCT cq.sku_group_id)", sql)

    def test_search_body_filters_sku_groups_and_enables_explain(self):
        body = self.search.build_search_body(
            query="bandana",
            sku_group_ids=[948376, 11],
            fields=["skus.title", "product.title.ru"],
            size=3000,
        )

        self.assertEqual(body["size"], 3000)
        self.assertTrue(body["explain"])
        self.assertEqual(
            body["query"]["function_score"]["query"]["bool"]["filter"],
            [{"terms": {"sku_group.id": [948376, 11]}}],
        )
        functions = body["query"]["function_score"]["functions"]
        self.assertTrue(functions)
        for function in functions:
            self.assertNotIn("modifier", function["field_value_factor"])

    def test_iceberg_commit_retry_handles_lock_errors(self):
        class WaitingForLockException(Exception):
            pass

        calls = []
        sleeps = []

        def flaky_commit():
            calls.append("commit")
            if len(calls) == 1:
                raise WaitingForLockException(
                    "Wait on lock for silver.feature_platform_search_query_sku_group_es_features"
                )
            return "ok"

        result = self.runtime._run_iceberg_commit(
            "test commit",
            flaky_commit,
            attempts=2,
            initial_sleep_seconds=0,
            sleep_fn=sleeps.append,
        )

        self.assertEqual(result, "ok")
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleeps, [0])

    def test_iceberg_commit_retry_does_not_swallow_non_lock_errors(self):
        with self.assertRaisesRegex(ValueError, "schema mismatch"):
            self.runtime._run_iceberg_commit(
                "test commit",
                lambda: (_ for _ in ()).throw(ValueError("schema mismatch")),
                attempts=2,
                initial_sleep_seconds=0,
                sleep_fn=lambda _: None,
            )

    def test_chunked_writer_stages_chunks_and_commits_once_after_collection(self):
        events = []

        class FakeAnalyze:
            @staticmethod
            def output_columns(fields):
                return ["query", "sku_group_id"]

        class FakeQueryGroups:
            def to_dict(self, orient):
                assert orient == "records"
                return [
                    {"query": "bandana", "sku_group_ids": [948376]},
                    {"query": "t-shirt", "sku_group_ids": [111]},
                ]

        class FakeTransaction:
            def commit_transaction(self):
                events.append("commit_transaction")

        class FakeTable:
            def name(self):
                return "silver.feature_platform_search_query_sku_group_es_features"

            def transaction(self):
                events.append("transaction")
                return FakeTransaction()

        class FakeFrame:
            def __init__(self, rows, columns):
                self.rows = rows
                self.columns = columns

        class FakePandas:
            DataFrame = FakeFrame

        def fake_collect_elasticsearch_rows(**_):
            events.append("collect")
            return [{"query": "bandana", "sku_group_id": 948376}]

        def fake_stage_clear_daily_snapshot(*_):
            events.append("stage_clear")

        def fake_append_daily_chunk(*_):
            events.append("stage_append")
            return 1

        def fake_run_iceberg_commit(_, operation, **__):
            events.append("commit")
            return operation()

        previous_pandas = sys.modules.get("pandas")
        previous_collect = self.runtime._collect_elasticsearch_rows
        previous_stage_clear = self.runtime.stage_clear_daily_snapshot
        previous_append = self.runtime.append_daily_chunk
        previous_commit = self.runtime._run_iceberg_commit
        previous_analyze = self.runtime._load_analyze_module
        sys.modules["pandas"] = FakePandas
        self.runtime._collect_elasticsearch_rows = fake_collect_elasticsearch_rows
        self.runtime.stage_clear_daily_snapshot = fake_stage_clear_daily_snapshot
        self.runtime.append_daily_chunk = fake_append_daily_chunk
        self.runtime._run_iceberg_commit = fake_run_iceberg_commit
        self.runtime._load_analyze_module = lambda: FakeAnalyze
        try:
            self.runtime.write_elasticsearch_features_by_chunks(
                table=FakeTable(),
                query_groups=FakeQueryGroups(),
                partition_date=date(2026, 3, 13),
                elastic=self.runtime.ElasticsearchConfig(
                    url="http://elasticsearch/_search",
                    auth=None,
                    headers={},
                ),
                search_module=object(),
                fields=["skus.title"],
                size=3000,
                parallel_jobs=2,
                chunk_size=1,
                timeout_seconds=60,
                retry_count=3,
            )
        finally:
            if previous_pandas is None:
                sys.modules.pop("pandas", None)
            else:
                sys.modules["pandas"] = previous_pandas
            self.runtime._collect_elasticsearch_rows = previous_collect
            self.runtime.stage_clear_daily_snapshot = previous_stage_clear
            self.runtime.append_daily_chunk = previous_append
            self.runtime._run_iceberg_commit = previous_commit
            self.runtime._load_analyze_module = previous_analyze

        self.assertEqual(
            events,
            [
                "transaction",
                "collect",
                "stage_clear",
                "stage_append",
                "collect",
                "stage_append",
                "commit",
                "commit_transaction",
            ],
        )

    def test_collect_elasticsearch_features_uses_parallel_jobs_and_parent_dedup(self):
        class FakeSearch:
            calls = []

            @staticmethod
            def build_search_body(query, sku_group_ids, fields, size):
                return {
                    "query": query,
                    "sku_group_ids": sku_group_ids,
                    "fields": fields,
                    "size": size,
                }

            @staticmethod
            def execute_search(
                url,
                body,
                auth,
                headers,
                timeout_seconds,
                retry_count,
            ):
                FakeSearch.calls.append(body)
                return {
                    "hits": {
                        "hits": [
                            {
                                "_source": {
                                    "sku_group": {
                                        "id": body["sku_group_ids"][0],
                                        "price": {"sell": 1000},
                                    },
                                    "product": {"id": 123, "title": {"ru": "Bandana"}},
                                    "query_encoder_v3": [],
                                },
                                "_explanation": {
                                    "value": 1.0,
                                    "description": "sum of:",
                                    "details": [],
                                },
                            }
                        ]
                    }
                }

        class FakeQueryGroups:
            def to_dict(self, orient):
                assert orient == "records"
                return [
                    {"query": "bandana", "sku_group_ids": [948376]},
                    {"query": "bandana", "sku_group_ids": [948376]},
                ]

        class FakeDataFrame:
            def __init__(self, rows, columns):
                self.rows = rows
                self.columns = columns

            def __len__(self):
                return len(self.rows)

            @property
            def loc(self):
                return self

            def __getitem__(self, key):
                row_index, column = key
                return self.rows[row_index][column]

        class FakePandas:
            DataFrame = FakeDataFrame

        previous_pandas = sys.modules.get("pandas")
        sys.modules["pandas"] = FakePandas
        try:
            frame = self.runtime.collect_elasticsearch_features(
                query_groups=FakeQueryGroups(),
                partition_date=date(2026, 3, 13),
                elastic=self.runtime.ElasticsearchConfig(
                    url="http://elasticsearch/_search",
                    auth=None,
                    headers={},
                ),
                search_module=FakeSearch,
                fields=["skus.title"],
                size=3000,
                parallel_jobs=2,
                timeout_seconds=60,
                retry_count=3,
            )
        finally:
            if previous_pandas is None:
                sys.modules.pop("pandas", None)
            else:
                sys.modules["pandas"] = previous_pandas

        self.assertEqual(len(FakeSearch.calls), 2)
        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.loc[0, "query"], "bandana")
        self.assertEqual(frame.loc[0, "sku_group_id"], 948376)


if __name__ == "__main__":
    unittest.main()
