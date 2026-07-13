import importlib.util
import sys
import types
from decimal import Decimal
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


class RecordingSkuGroupToPromoFeatureSet(RecordingFeatureSet):
    pass


def _install_stubs():
    pyspark_module = types.ModuleType("pyspark")
    pyspark_sql_module = types.ModuleType("pyspark.sql")
    pyspark_sql_module.DataFrame = object
    pyspark_sql_module.SparkSession = object
    pyspark_sql_functions_module = types.ModuleType("pyspark.sql.functions")
    pyspark_sql_types_module = types.ModuleType("pyspark.sql.types")
    pyspark_sql_types_module.BinaryType = object
    pyspark_sql_module.functions = pyspark_sql_functions_module

    ranking_module = types.ModuleType("ranking_python_client")
    for class_name in (
        "AccountFeatureSet",
        "AccountToCategoryFeatureSet",
        "QueryFeatureSet",
        "SkuGroupFeatureSet",
        "SkuGroupToCategoryFeatureSet",
        "SkuGroupToQueryFeatureSet",
    ):
        setattr(ranking_module, class_name, type(class_name, (RecordingFeatureSet,), {}))
    ranking_module.SkuGroupToPromoFeatureSet = RecordingSkuGroupToPromoFeatureSet
    ranking_module.FeaturesUpdate = RecordingFeaturesUpdate

    sys.modules["pyspark"] = pyspark_module
    sys.modules["pyspark.sql"] = pyspark_sql_module
    sys.modules["pyspark.sql.functions"] = pyspark_sql_functions_module
    sys.modules["pyspark.sql.types"] = pyspark_sql_types_module
    sys.modules["ranking_python_client"] = ranking_module


def _load_upload_module():
    _install_stubs()
    sys.path.insert(0, str(UPLOAD_ROOT))
    module_path = UPLOAD_ROOT / "job" / "upload_ranking_features.py"
    spec = importlib.util.spec_from_file_location(
        "test_upload_ranking_features",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_sku_group_promotion_source_uses_sku_group_to_promo_proto():
    upload = _load_upload_module()
    row = {
        "sku_group_id": 123,
        "promotion_id": "123",
        "avg_sell_price": Decimal("1.234"),
    }
    feature_group = {
        "name": "cool_skg_promo_features",
        "features": ["avg_sell_price"],
    }
    metadata = {"entity_keys": ["sku_group_id", "promotion_id"]}

    assert upload._row_to_proto(row, feature_group, metadata) == b"serialized"

    assert RecordingSkuGroupToPromoFeatureSet.last_kwargs == {
        "skuGroupId": 123,
        "promoId": "123",
        "fsName": "cool_skg_promo_features",
        "features": [1.234],
    }
    assert list(RecordingFeaturesUpdate.last_kwargs) == ["skuGroupToPromoFeatureSet"]
    assert isinstance(
        RecordingFeaturesUpdate.last_kwargs["skuGroupToPromoFeatureSet"],
        RecordingSkuGroupToPromoFeatureSet,
    )
