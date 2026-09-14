import importlib.util
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UPLOAD_ROOT = ROOT / "upload" / "features_service_upload" / "v1"


class RecordingFeatureSet:
    last_kwargs = None

    def __init__(self, **kwargs):
        type(self).last_kwargs = kwargs


class RecordingFeaturesUpdate:
    last_kwargs = None

    def __init__(self, **kwargs):
        type(self).last_kwargs = kwargs

    def SerializeToString(self):
        return b"serialized"


class RecordingSkuGroupCategoryToQueryFeatureSet(RecordingFeatureSet):
    pass


class FakeColumn:
    def __init__(self, expression):
        self.expression = expression

    def desc(self):
        return FakeColumn(("desc", self.expression))

    def cast(self, type_name):
        return FakeColumn(("cast", self.expression, type_name))

    def over(self, window):
        return FakeColumn(("over", self.expression, window))

    def __eq__(self, other):
        other_expression = other.expression if isinstance(other, FakeColumn) else other
        return FakeColumn(("eq", self.expression, other_expression))

    def __repr__(self):
        return f"FakeColumn({self.expression!r})"


class FakeWindowSpec:
    def __init__(self, partition_by=(), order_by=()):
        self.partition_by = partition_by
        self.order_by = order_by

    def orderBy(self, *columns):
        return FakeWindowSpec(self.partition_by, tuple(c.expression for c in columns))


class FakeWindow:
    @staticmethod
    def partitionBy(*columns):
        return FakeWindowSpec(tuple(c.expression for c in columns))


class FakeNa:
    def __init__(self, frame):
        self.frame = frame

    def fill(self, value, subset):
        self.frame.calls.append(("na.fill", value, tuple(subset)))
        return self.frame


class FakeFrame:
    def __init__(self):
        self.calls = []

    def filter(self, condition):
        self.calls.append(("filter", condition.expression))
        return self

    def withColumn(self, name, column):
        self.calls.append(("withColumn", name, column.expression))
        return self

    def drop(self, *names):
        self.calls.append(("drop", names))
        return self

    def select(self, *columns):
        self.calls.append(("select", tuple(c.expression for c in columns)))
        return self

    def limit(self, value):
        self.calls.append(("limit", value))
        return self

    def join(self, other, on, how):
        self.calls.append(("join", other, on, how))
        return self

    def unionByName(self, other):
        self.calls.append(("unionByName", other))
        return self

    @property
    def na(self):
        return FakeNa(self)


class FakeSpark:
    def __init__(self, frame, frames=None):
        self.frame = frame
        self.frames = frames or {}
        self.tables = []

    def table(self, name):
        self.tables.append(name)
        return self.frames.get(name, self.frame)


def _install_stubs():
    pyspark_module = types.ModuleType("pyspark")
    pyspark_sql_module = types.ModuleType("pyspark.sql")
    pyspark_sql_module.DataFrame = object
    pyspark_sql_module.SparkSession = object
    pyspark_sql_module.Window = FakeWindow
    functions = types.ModuleType("pyspark.sql.functions")
    functions.col = FakeColumn
    functions.lit = lambda value: FakeColumn(("lit", value))
    functions.row_number = lambda: FakeColumn(("row_number",))
    functions.lower = lambda column: FakeColumn(("lower", column.expression))
    pyspark_sql_types_module = types.ModuleType("pyspark.sql.types")
    pyspark_sql_types_module.BinaryType = object
    pyspark_sql_module.functions = functions

    ranking_module = types.ModuleType("ranking_python_client")
    for class_name in (
        "AccountFeatureSet",
        "AccountToCategoryFeatureSet",
        "QueryFeatureSet",
        "SkuGroupFeatureSet",
        "SkuGroupToCategoryFeatureSet",
        "SkuGroupToQueryFeatureSet",
        "SkuGroupToPromoFeatureSet",
    ):
        setattr(ranking_module, class_name, type(class_name, (RecordingFeatureSet,), {}))
    ranking_module.SkuGroupCategoryToQueryFeatureSet = (
        RecordingSkuGroupCategoryToQueryFeatureSet
    )
    ranking_module.FeaturesUpdate = RecordingFeaturesUpdate

    sys.modules["pyspark"] = pyspark_module
    sys.modules["pyspark.sql"] = pyspark_sql_module
    sys.modules["pyspark.sql.functions"] = functions
    sys.modules["pyspark.sql.types"] = pyspark_sql_types_module
    sys.modules["ranking_python_client"] = ranking_module


def _load_upload_module():
    _install_stubs()
    sys.path.insert(0, str(UPLOAD_ROOT))
    module_path = UPLOAD_ROOT / "job" / "upload_ranking_features.py"
    spec = importlib.util.spec_from_file_location(
        "test_upload_ranking_features_category_to_query",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


FEATURE_GROUP = {
    "name": "query_category_relevance",
    "features": ["relevance"],
    "source": {
        "schema": "gold",
        "table": "feature_platform_query_category_relevance",
        "read_mode": "full_table",
    },
}
METADATA = {
    "catalog": "iceberg",
    "primary_key": ["date", "category_id", "query_text"],
    "date_column": "date",
    "timestamp_column": None,
    "entity_keys": ["category_id", "query_text"],
}


def test_category_query_source_uses_sku_group_category_to_query_proto():
    upload = _load_upload_module()
    row = {"category_id": 42, "query_text": "телефон", "relevance": 2}

    assert upload._row_to_proto(row, FEATURE_GROUP, METADATA) == b"serialized"

    assert RecordingSkuGroupCategoryToQueryFeatureSet.last_kwargs == {
        "skuGroupCategoryId": 42,
        "query": "телефон",
        "fsName": "query_category_relevance",
        "features": [2.0],
    }
    assert list(RecordingFeaturesUpdate.last_kwargs) == [
        "skuGroupCategoryToQueryFeatureSet"
    ]


def test_null_relevance_is_sent_as_zero():
    upload = _load_upload_module()
    row = {"category_id": 42, "query_text": "телефон", "relevance": None}

    upload._row_to_proto(row, FEATURE_GROUP, METADATA)

    assert RecordingSkuGroupCategoryToQueryFeatureSet.last_kwargs["features"] == [0.0]


def test_other_category_entities_keep_category_id_argument():
    """Переопределение ключа — только для коллекции категория × запрос."""
    upload = _load_upload_module()
    row = {"category_id": 7, "sku_group_id": 9, "x": 1}
    metadata = {"entity_keys": ["sku_group_id", "category_id"]}

    upload._row_to_proto(row, {"name": "fs", "features": ["x"]}, metadata)

    recorded = sys.modules["ranking_python_client"].SkuGroupToCategoryFeatureSet.last_kwargs
    assert recorded["categoryId"] == 7
    assert recorded["skuGroupId"] == 9


def test_full_table_read_keeps_latest_date_per_key_without_date_filter():
    upload = _load_upload_module()
    frame = FakeFrame()
    spark = FakeSpark(frame)

    upload._prepare_source_frame(spark, FEATURE_GROUP, METADATA, "2026-09-14")

    assert spark.tables == ["iceberg.gold.feature_platform_query_category_relevance"]
    filters = [call for call in frame.calls if call[0] == "filter"]
    assert all(
        "2026-09-14" not in repr(condition) for _, condition in filters
    ), frame.calls
    rank_columns = [call for call in frame.calls if call[0] == "withColumn"]
    assert len(rank_columns) == 1, frame.calls
    _, rank_name, rank_expression = rank_columns[0]
    assert rank_expression[0] == "over"
    assert rank_expression[1] == ("row_number",)
    window = rank_expression[2]
    assert window.partition_by == ("category_id", "query_text")
    assert window.order_by == (("desc", "date"),)
    assert ("filter", ("eq", rank_name, 1)) in frame.calls
    assert frame.calls[-2] == ("select", ("category_id", "query_text", "relevance"))
    assert frame.calls[-1] == ("na.fill", 0.0, ("relevance",))
    assert not [
        call for call in frame.calls if call[0] in ("join", "unionByName")
    ], frame.calls


def test_full_table_read_requires_date_column():
    upload = _load_upload_module()
    metadata = {**METADATA, "date_column": None}

    try:
        upload._prepare_source_frame(FakeSpark(FakeFrame()), FEATURE_GROUP, metadata, "2026-09-14")
    except ValueError as error:
        assert "full_table" in str(error)
    else:
        raise AssertionError("full_table без колонки date должен падать")


QUERY_ID_DICTIONARY_TABLE = "iceberg.gold.feature_platform_search_query_id"


def test_query_id_dictionary_adds_query_texts_and_lowercases_them():
    """Метка query_id уходит и на формулировки из справочника; текст — в нижнем регистре."""
    upload = _load_upload_module()
    source = FakeFrame()
    dictionary = FakeFrame()
    spark = FakeSpark(source, {QUERY_ID_DICTIONARY_TABLE: dictionary})
    feature_group = {
        **FEATURE_GROUP,
        "source": {
            **FEATURE_GROUP["source"],
            "query_id_dictionary": {
                "schema": "gold",
                "table": "feature_platform_search_query_id",
            },
        },
    }

    upload._prepare_source_frame(spark, feature_group, METADATA, "2026-09-14")

    assert spark.tables == [
        "iceberg.gold.feature_platform_query_category_relevance",
        QUERY_ID_DICTIONARY_TABLE,
    ]
    assert dictionary.calls == [("select", ("query_id", "query_text"))]
    merge_steps = [
        ("drop", ("query_text",)),
        ("join", dictionary, "query_id", "inner"),
        ("unionByName", source),
        ("withColumn", "query_text", ("lower", "query_text")),
    ]
    positions = [source.calls.index(step) for step in merge_steps]
    assert positions == sorted(positions), source.calls
    column_calls = [call[1] for call in source.calls if call[0] == "withColumn"]
    # Регистр приводится до выбора самой свежей даты, и других преобразований нет.
    assert column_calls == ["query_text", upload.LATEST_DATE_RANK_COLUMN], source.calls


def main() -> int:
    test_category_query_source_uses_sku_group_category_to_query_proto()
    test_null_relevance_is_sent_as_zero()
    test_other_category_entities_keep_category_id_argument()
    test_full_table_read_keeps_latest_date_per_key_without_date_filter()
    test_full_table_read_requires_date_column()
    test_query_id_dictionary_adds_query_texts_and_lowercases_them()
    print("Ranking upload category-to-query tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
