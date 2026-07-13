import gzip
import io
import importlib.util
import json
import sys
import types
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
        self.assertEqual(
            self.runtime.parse_partition_date("2026-06-16"),
            date(2026, 6, 16),
        )

    def test_raw_storage_paths_use_configured_prefix_and_safe_run_id(self):
        storage = self.runtime.RawStorageConfig(
            conn_id="search_research_bucket",
            bucket="bucket",
            prefix="airflow/2026/bm25_features",
            endpoint_url=None,
            region_name="ru-central1",
            access_key_id=None,
            secret_access_key=None,
        )
        partition_date = date(2026, 7, 8)

        self.assertEqual(
            self.runtime.raw_date_prefix(storage, partition_date),
            "airflow/2026/bm25_features/raw/date=2026-07-08",
        )
        self.assertEqual(
            self.runtime.raw_run_prefix(
                storage,
                partition_date,
                "manual__2026-07-08T10:40:23.147933+00:00",
            ),
            "airflow/2026/bm25_features/raw/date=2026-07-08/"
            "run_id=manual__2026-07-08T10:40:23.147933_00:00",
        )
        self.assertEqual(
            self.runtime.raw_date_manifest_key(storage, partition_date),
            "airflow/2026/bm25_features/raw/date=2026-07-08/manifest.json",
        )
        self.assertEqual(
            self.runtime.prepared_run_prefix(
                storage,
                partition_date,
                "scheduled__2026-07-08T04:00:00+00:00",
            ),
            "airflow/2026/bm25_features/prepared/date=2026-07-08/"
            "run_id=scheduled__2026-07-08T04:00:00_00:00",
        )

    def test_load_latest_raw_manifest_falls_back_to_run_manifest(self):
        storage = self.runtime.RawStorageConfig(
            conn_id="search_research_bucket",
            bucket="bucket",
            prefix="airflow/2026/bm25_features",
            endpoint_url=None,
            region_name="ru-central1",
            access_key_id=None,
            secret_access_key=None,
        )
        partition_date = date(2026, 7, 8)
        run_manifest_key = (
            "airflow/2026/bm25_features/raw/date=2026-07-08/"
            "run_id=scheduled__2026-07-08T04:00:00_00:00/manifest.json"
        )

        class FakeNoSuchKey(Exception):
            def __init__(self, key):
                super().__init__(f"NoSuchKey: {key}")
                self.response = {"Error": {"Code": "NoSuchKey"}}

        class FakePaginator:
            def paginate(self, Bucket, Prefix):
                self.bucket = Bucket
                self.prefix = Prefix
                return [
                    {
                        "Contents": [
                            {
                                "Key": run_manifest_key,
                                "LastModified": "2026-07-08T06:00:00+00:00",
                            }
                        ]
                    }
                ]

        class FakeClient:
            def __init__(self):
                self.read_keys = []

            def get_object(self, Bucket, Key):
                self.read_keys.append(Key)
                if Key != run_manifest_key:
                    raise FakeNoSuchKey(Key)
                return {
                    "Body": io.BytesIO(
                        json.dumps(
                            {
                                "date": "2026-07-08",
                                "parts": [{"key": "part-000001.jsonl.gz"}],
                            }
                        ).encode("utf-8")
                    )
                }

            def get_paginator(self, name):
                self.paginator_name = name
                return FakePaginator()

        client = FakeClient()
        manifest = self.runtime.load_latest_raw_manifest(client, storage, partition_date)

        self.assertEqual(manifest["date"], "2026-07-08")
        self.assertEqual(manifest["parts"][0]["key"], "part-000001.jsonl.gz")
        self.assertEqual(client.paginator_name, "list_objects_v2")
        self.assertEqual(
            client.read_keys,
            [
                "airflow/2026/bm25_features/raw/date=2026-07-08/manifest.json",
                run_manifest_key,
            ],
        )

    def test_load_latest_raw_manifest_falls_back_to_latest_run_parts(self):
        storage = self.runtime.RawStorageConfig(
            conn_id="search_research_bucket",
            bucket="bucket",
            prefix="airflow/2026/bm25_features",
            endpoint_url=None,
            region_name="ru-central1",
            access_key_id=None,
            secret_access_key=None,
        )
        partition_date = date(2026, 7, 8)

        class FakeNoSuchKey(Exception):
            def __init__(self, key):
                super().__init__(f"NoSuchKey: {key}")
                self.response = {"Error": {"Code": "NoSuchKey"}}

        class FakePaginator:
            def paginate(self, Bucket, Prefix):
                return [
                    {
                        "Contents": [
                            {
                                "Key": (
                                    "airflow/2026/bm25_features/raw/date=2026-07-08/"
                                    "run_id=scheduled__2026-07-07T04:00:00_00:00/"
                                    "chunk=000001/part-000001.jsonl.gz"
                                ),
                                "LastModified": "2026-07-08T05:00:00+00:00",
                                "Size": 10,
                            },
                            {
                                "Key": (
                                    "airflow/2026/bm25_features/raw/date=2026-07-08/"
                                    "run_id=scheduled__2026-07-08T04:00:00_00:00/"
                                    "chunk=000002/part-000001.jsonl.gz"
                                ),
                                "LastModified": "2026-07-08T06:02:00+00:00",
                                "Size": 20,
                            },
                            {
                                "Key": (
                                    "airflow/2026/bm25_features/raw/date=2026-07-08/"
                                    "run_id=scheduled__2026-07-08T04:00:00_00:00/"
                                    "chunk=000001/part-000001.jsonl.gz"
                                ),
                                "LastModified": "2026-07-08T06:01:00+00:00",
                                "Size": 30,
                            },
                        ]
                    }
                ]

        class FakeClient:
            def get_object(self, Bucket, Key):
                raise FakeNoSuchKey(Key)

            def get_paginator(self, name):
                return FakePaginator()

        manifest = self.runtime.load_latest_raw_manifest(
            FakeClient(),
            storage,
            partition_date,
        )

        self.assertEqual(manifest["date"], "2026-07-08")
        self.assertEqual(manifest["source"], "listed_raw_parts")
        self.assertEqual(manifest["run_id"], "scheduled__2026-07-08T04:00:00_00:00")
        self.assertEqual(
            [part["key"] for part in manifest["parts"]],
            [
                "airflow/2026/bm25_features/raw/date=2026-07-08/"
                "run_id=scheduled__2026-07-08T04:00:00_00:00/"
                "chunk=000001/part-000001.jsonl.gz",
                "airflow/2026/bm25_features/raw/date=2026-07-08/"
                "run_id=scheduled__2026-07-08T04:00:00_00:00/"
                "chunk=000002/part-000001.jsonl.gz",
            ],
        )

    def test_load_latest_raw_manifest_requires_manifest_or_raw_parts(self):
        storage = self.runtime.RawStorageConfig(
            conn_id="search_research_bucket",
            bucket="bucket",
            prefix="airflow/2026/bm25_features",
            endpoint_url=None,
            region_name="ru-central1",
            access_key_id=None,
            secret_access_key=None,
        )

        class FakeNoSuchKey(Exception):
            def __init__(self, key):
                super().__init__(f"NoSuchKey: {key}")
                self.response = {"Error": {"Code": "NoSuchKey"}}

        class FakePaginator:
            def paginate(self, Bucket, Prefix):
                return [{"Contents": []}]

        class FakeClient:
            def get_object(self, Bucket, Key):
                raise FakeNoSuchKey(Key)

            def get_paginator(self, name):
                return FakePaginator()

        with self.assertRaisesRegex(RuntimeError, "Raw Elasticsearch data was not found"):
            self.runtime.load_latest_raw_manifest(
                FakeClient(),
                storage,
                date(2026, 7, 8),
            )

    def test_load_latest_prepared_manifest_falls_back_to_latest_run_parts(self):
        storage = self.runtime.RawStorageConfig(
            conn_id="search_research_bucket",
            bucket="bucket",
            prefix="airflow/2026/bm25_features",
            endpoint_url=None,
            region_name="ru-central1",
            access_key_id=None,
            secret_access_key=None,
        )

        class FakeNoSuchKey(Exception):
            def __init__(self, key):
                super().__init__(f"NoSuchKey: {key}")
                self.response = {"Error": {"Code": "NoSuchKey"}}

        class FakePaginator:
            def paginate(self, Bucket, Prefix):
                return [
                    {
                        "Contents": [
                            {
                                "Key": (
                                    "airflow/2026/bm25_features/prepared/date=2026-07-08/"
                                    "run_id=scheduled__2026-07-07T04:00:00_00:00/"
                                    "chunk=000001/part-000001.parquet"
                                ),
                                "LastModified": "2026-07-08T05:00:00+00:00",
                                "Size": 10,
                            },
                            {
                                "Key": (
                                    "airflow/2026/bm25_features/prepared/date=2026-07-08/"
                                    "run_id=scheduled__2026-07-08T04:00:00_00:00/"
                                    "chunk=000002/part-000001.parquet"
                                ),
                                "LastModified": "2026-07-08T06:02:00+00:00",
                                "Size": 20,
                            },
                            {
                                "Key": (
                                    "airflow/2026/bm25_features/prepared/date=2026-07-08/"
                                    "run_id=scheduled__2026-07-08T04:00:00_00:00/"
                                    "chunk=000001/part-000001.parquet"
                                ),
                                "LastModified": "2026-07-08T06:01:00+00:00",
                                "Size": 30,
                            },
                        ]
                    }
                ]

        class FakeClient:
            def get_object(self, Bucket, Key):
                raise FakeNoSuchKey(Key)

            def get_paginator(self, name):
                return FakePaginator()

        manifest = self.runtime.load_latest_prepared_manifest(
            FakeClient(),
            storage,
            date(2026, 7, 8),
        )

        self.assertEqual(manifest["date"], "2026-07-08")
        self.assertEqual(manifest["source"], "listed_prepared_parts")
        self.assertEqual(manifest["run_id"], "scheduled__2026-07-08T04:00:00_00:00")
        self.assertEqual(
            [part["key"] for part in manifest["parts"]],
            [
                "airflow/2026/bm25_features/prepared/date=2026-07-08/"
                "run_id=scheduled__2026-07-08T04:00:00_00:00/"
                "chunk=000001/part-000001.parquet",
                "airflow/2026/bm25_features/prepared/date=2026-07-08/"
                "run_id=scheduled__2026-07-08T04:00:00_00:00/"
                "chunk=000002/part-000001.parquet",
            ],
        )

    def test_iter_raw_manifest_records_reads_jsonl_gzip_parts(self):
        storage = self.runtime.RawStorageConfig(
            conn_id="search_research_bucket",
            bucket="bucket",
            prefix="airflow/2026/bm25_features",
            endpoint_url=None,
            region_name="ru-central1",
            access_key_id=None,
            secret_access_key=None,
        )
        payload = io.BytesIO()
        with gzip.GzipFile(fileobj=payload, mode="wb") as stream:
            for record in [
                {"query": "bandana", "hit": {"_id": "1"}},
                {"query": "t-shirt", "hit": {"_id": "2"}},
            ]:
                stream.write(json.dumps(record).encode("utf-8") + b"\n")
        payload.seek(0)

        class FakeClient:
            def get_object(self, Bucket, Key):
                self.bucket = Bucket
                self.key = Key
                return {"Body": payload}

        client = FakeClient()
        records = list(
            self.runtime.iter_raw_manifest_records(
                client,
                storage,
                {"parts": [{"key": "raw/date=2026-07-08/part.jsonl.gz"}]},
            )
        )

        self.assertEqual(client.bucket, "bucket")
        self.assertEqual(client.key, "raw/date=2026-07-08/part.jsonl.gz")
        self.assertEqual([record["query"] for record in records], ["bandana", "t-shirt"])

    def test_write_raw_features_to_prepared_parquet_uses_s3_without_iceberg(self):
        storage = self.runtime.RawStorageConfig(
            conn_id="search_research_bucket",
            bucket="bucket",
            prefix="airflow/2026/bm25_features",
            endpoint_url=None,
            region_name="ru-central1",
            access_key_id=None,
            secret_access_key=None,
        )
        raw_key = (
            "airflow/2026/bm25_features/raw/date=2026-07-08/"
            "run_id=scheduled__2026-07-08T04:00:00_00:00/"
            "chunk=000001/part-000001.jsonl.gz"
        )
        raw_payload = io.BytesIO()
        with gzip.GzipFile(fileobj=raw_payload, mode="wb") as stream:
            for record in [
                {"query": "bandana", "hit": {"sku_group_id": 948376}},
                {"query": "ignored", "hit": {"sku_group_id": 0}},
            ]:
                stream.write(json.dumps(record).encode("utf-8") + b"\n")
        raw_payload.seek(0)

        class FakeAnalyze:
            @staticmethod
            def output_columns(fields):
                return ["date", "query", "sku_group_id"]

            @staticmethod
            def hit_to_row(hit, query, partition_date, fields):
                return {
                    "date": partition_date,
                    "query": query,
                    "sku_group_id": hit["sku_group_id"],
                }

        class FakeClient:
            def __init__(self):
                self.objects = {}

            def get_object(self, Bucket, Key):
                self.bucket = Bucket
                self.key = Key
                return {"Body": io.BytesIO(raw_payload.getvalue())}

            def put_object(self, Bucket, Key, Body, ContentType):
                self.objects[Key] = {
                    "body": Body,
                    "content_type": ContentType,
                }

        uploaded = []
        events = []
        client = FakeClient()

        def fake_upload_prepared_row_buffer(
            _client,
            _storage,
            key,
            rows,
            partition_date,
            columns,
        ):
            uploaded.append(
                {
                    "key": key,
                    "rows": list(rows),
                    "partition_date": partition_date,
                    "columns": list(columns),
                }
            )
            return {"key": key, "rows": len(rows), "bytes": 123}

        previous_get_s3 = self.runtime.get_s3_client
        previous_load_manifest = self.runtime.load_latest_raw_manifest
        previous_delete_prefix = self.runtime.delete_s3_prefix
        previous_clear_markers = self.runtime.clear_prepared_success_markers
        previous_analyze = self.runtime._load_analyze_module
        previous_upload = self.runtime._upload_prepared_row_buffer
        self.runtime.get_s3_client = lambda _storage: client
        self.runtime.load_latest_raw_manifest = lambda *_: {
            "date": "2026-07-08",
            "run_id": "scheduled__2026-07-08T04:00:00_00:00",
            "run_prefix": (
                "airflow/2026/bm25_features/raw/date=2026-07-08/"
                "run_id=scheduled__2026-07-08T04:00:00_00:00"
            ),
            "parts": [{"key": raw_key}],
        }
        self.runtime.delete_s3_prefix = lambda _client, _storage, prefix: events.append(
            ("delete", prefix)
        )
        self.runtime.clear_prepared_success_markers = lambda *_: events.append(("clear",))
        self.runtime._load_analyze_module = lambda: FakeAnalyze
        self.runtime._upload_prepared_row_buffer = fake_upload_prepared_row_buffer
        try:
            manifest = self.runtime.write_raw_features_to_prepared_parquet(
                partition_date=date(2026, 7, 8),
                storage=storage,
                fields=["skus.title"],
                write_chunk_size=10,
            )
        finally:
            self.runtime.get_s3_client = previous_get_s3
            self.runtime.load_latest_raw_manifest = previous_load_manifest
            self.runtime.delete_s3_prefix = previous_delete_prefix
            self.runtime.clear_prepared_success_markers = previous_clear_markers
            self.runtime._load_analyze_module = previous_analyze
            self.runtime._upload_prepared_row_buffer = previous_upload

        prepared_prefix = (
            "airflow/2026/bm25_features/prepared/date=2026-07-08/"
            "run_id=scheduled__2026-07-08T04:00:00_00:00"
        )
        self.assertEqual(events, [("delete", prepared_prefix + "/"), ("clear",)])
        self.assertEqual(manifest["rows"], 1)
        self.assertEqual(manifest["raw_records"], 2)
        self.assertEqual(
            uploaded[0]["key"],
            prepared_prefix + "/chunk=000001/part-000001.parquet",
        )
        self.assertEqual(uploaded[0]["rows"][0]["sku_group_id"], 948376)
        self.assertIn(prepared_prefix + "/manifest.json", client.objects)
        self.assertIn(
            "airflow/2026/bm25_features/prepared/date=2026-07-08/manifest.json",
            client.objects,
        )
        self.assertIn(
            "airflow/2026/bm25_features/prepared/date=2026-07-08/_SUCCESS",
            client.objects,
        )

    def test_write_prepared_parquet_to_iceberg_commits_once(self):
        storage = self.runtime.RawStorageConfig(
            conn_id="search_research_bucket",
            bucket="bucket",
            prefix="airflow/2026/bm25_features",
            endpoint_url=None,
            region_name="ru-central1",
            access_key_id=None,
            secret_access_key=None,
        )
        parts = [
            {"key": "prepared/date=2026-07-08/run_id=run/chunk=000001/part-000001.parquet"},
            {"key": "prepared/date=2026-07-08/run_id=run/chunk=000002/part-000001.parquet"},
        ]
        events = []

        class FakeEqualTo:
            def __init__(self, name, value):
                self.name = name
                self.value = value

        class FakeArrowTable:
            def __init__(self, name, rows):
                self.name = name
                self.num_rows = rows

        class FakeTransaction:
            def delete(self, delete_filter):
                events.append(("delete", delete_filter.name, delete_filter.value))

            def append(self, arrow_table):
                events.append(("append", arrow_table.name, arrow_table.num_rows))

            def commit_transaction(self):
                events.append(("commit_transaction",))

        class FakeTable:
            def name(self):
                return "silver.feature_platform_search_query_sku_group_es_features"

            def transaction(self):
                events.append(("transaction",))
                return FakeTransaction()

        pyiceberg_module = types.ModuleType("pyiceberg")
        expressions_module = types.ModuleType("pyiceberg.expressions")
        expressions_module.EqualTo = FakeEqualTo

        previous_pyiceberg = sys.modules.get("pyiceberg")
        previous_expressions = sys.modules.get("pyiceberg.expressions")
        previous_get_s3 = self.runtime.get_s3_client
        previous_manifest = self.runtime.load_latest_prepared_manifest
        previous_read = self.runtime._read_prepared_parquet_part
        previous_align = self.runtime._align_arrow_table_to_iceberg_schema
        previous_commit = self.runtime._run_iceberg_commit
        sys.modules["pyiceberg"] = pyiceberg_module
        sys.modules["pyiceberg.expressions"] = expressions_module
        self.runtime.get_s3_client = lambda _storage: object()
        self.runtime.load_latest_prepared_manifest = lambda *_: {
            "date": "2026-07-08",
            "run_id": "run",
            "parts": parts,
        }

        def fake_read(_client, _storage, part):
            events.append(("read", part["key"]))
            return FakeArrowTable(part["key"], 10)

        self.runtime._read_prepared_parquet_part = fake_read
        self.runtime._align_arrow_table_to_iceberg_schema = (
            lambda _table, arrow_table, _partition_date: arrow_table
        )
        self.runtime._run_iceberg_commit = lambda _name, operation, **__: (
            events.append(("commit", _name)),
            operation(),
        )[-1]
        try:
            self.runtime.write_prepared_parquet_to_iceberg(
                table=FakeTable(),
                partition_date=date(2026, 7, 8),
                storage=storage,
            )
        finally:
            if previous_pyiceberg is None:
                sys.modules.pop("pyiceberg", None)
            else:
                sys.modules["pyiceberg"] = previous_pyiceberg
            if previous_expressions is None:
                sys.modules.pop("pyiceberg.expressions", None)
            else:
                sys.modules["pyiceberg.expressions"] = previous_expressions
            self.runtime.get_s3_client = previous_get_s3
            self.runtime.load_latest_prepared_manifest = previous_manifest
            self.runtime._read_prepared_parquet_part = previous_read
            self.runtime._align_arrow_table_to_iceberg_schema = previous_align
            self.runtime._run_iceberg_commit = previous_commit

        self.assertEqual(
            events,
            [
                ("transaction",),
                ("delete", "date", date(2026, 7, 8)),
                ("read", parts[0]["key"]),
                ("append", parts[0]["key"], 10),
                ("read", parts[1]["key"]),
                ("append", parts[1]["key"], 10),
                (
                    "commit",
                    "commit prepared parquet load "
                    "silver.feature_platform_search_query_sku_group_es_features "
                    "date=2026-07-08",
                ),
                ("commit_transaction",),
            ],
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
            min_result_query_installs=2,
        )

        self.assertIn('FROM "dwh-iceberg".silver_b2c_clickstream.events', sql)
        self.assertIn("event_type = 'PRODUCT_IMPRESSION'", sql)
        self.assertIn("widget_space_name = 'SEARCH_RESULTS'", sql)
        self.assertIn("logged_at >= CAST('2026-03-13' AS TIMESTAMP(6))", sql)
        self.assertIn("logged_at < CAST('2026-03-14' AS TIMESTAMP(6))", sql)
        self.assertIn("received_at >= CAST('2026-03-13' AS TIMESTAMP(6))", sql)
        self.assertIn("received_at < CAST('2026-03-14' AS TIMESTAMP(6))", sql)
        self.assertNotIn("logged_at >= CAST('2026-03-10' AS TIMESTAMP(6))", sql)
        self.assertIn("logged_at >= CAST('2026-03-12' AS TIMESTAMP(6))", sql)
        self.assertIn("HAVING", sql)
        self.assertIn("COUNT(DISTINCT install_id) >= 2", sql)
        self.assertIn("INNER JOIN final_stats fs", sql)
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

        collect_results = iter(
            [
                [{"query": "bandana", "sku_group_id": 948376}],
                [{"query": "t-shirt", "sku_group_id": 111}],
            ]
        )

        def fake_iter_elasticsearch_rows(**_):
            events.append("collect")
            yield from next(collect_results)

        def fake_stage_clear_daily_snapshot(*_):
            events.append("stage_clear")

        def fake_append_daily_chunk(*_):
            events.append("stage_append")
            return 1

        def fake_run_iceberg_commit(_, operation, **__):
            events.append("commit")
            return operation()

        previous_pandas = sys.modules.get("pandas")
        previous_iter = self.runtime._iter_elasticsearch_rows
        previous_stage_clear = self.runtime.stage_clear_daily_snapshot
        previous_append = self.runtime.append_daily_chunk
        previous_commit = self.runtime._run_iceberg_commit
        previous_analyze = self.runtime._load_analyze_module
        sys.modules["pandas"] = FakePandas
        self.runtime._iter_elasticsearch_rows = fake_iter_elasticsearch_rows
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
                write_chunk_size=1,
                timeout_seconds=60,
                retry_count=3,
            )
        finally:
            if previous_pandas is None:
                sys.modules.pop("pandas", None)
            else:
                sys.modules["pandas"] = previous_pandas
            self.runtime._iter_elasticsearch_rows = previous_iter
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
