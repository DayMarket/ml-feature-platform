"""Проверить форму независимых raw captures SKU, category и MDM."""

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from layers.silver.sku_id.demand_catalog_sku.v1.job.query import capture_query

ROOT = Path(__file__).resolve().parents[1]
CONFIG = yaml.safe_load((ROOT / "layers/silver/sku_id/demand_catalog_sku/v1/config.yaml").read_text())


@pytest.mark.parametrize("kind", ["sku", "category", "golden", "active_links"])
def test_metadata_and_full_capture_share_projection(kind):
    full = capture_query(CONFIG, kind)
    meta = capture_query(CONFIG, kind, metadata_only=True)
    assert "LIMIT" not in full and " LIMIT 0 SETTINGS" in meta and " ORDER BY " not in meta
    assert full.split(" ORDER BY ")[0] == meta.split(" LIMIT 0 ")[0]
    assert "max_threads=1, max_execution_time=300" in full


def test_mdm_source_means_link_provenance_and_orphans_are_not_lost():
    sql = capture_query(CONFIG, "active_links")
    assert "source AS link_provenance" in sql and " WHERE deleted_at IS NULL" in sql
    assert "dictHas('matching.meta_sku_id_dict', meta_sku_id)" in sql
    assert " AS meta_present" in sql and " AS meta_source" in sql
    assert "INNER JOIN" not in sql and "'uzum'" not in sql
    assert "CAST(NULL AS Nullable(String))" in sql


def test_category_and_sku_preserve_source_values():
    category = capture_query(CONFIG, "category")
    sku = capture_query(CONFIG, "sku")
    assert "l6_category AS raw_l6_category_id" in category and "title_ru AS leaf_title" in category
    assert "WHERE" not in sku and "product_id, category_id, seller_id, shop_id" in sku
    assert "toTimeZone(created_at, 'UTC')" in sku and "toInt64(id)" not in sku
    assert " FINAL" in capture_query(CONFIG, "golden")


@pytest.mark.parametrize("key,kind", [("sku", "sku"), ("category", "category"),
    ("golden", "golden"), ("meta_sku", "active_links"), ("golden_links", "active_links")])
@pytest.mark.parametrize("value", ["wrong", "a.b.c", "a.'b", None])
def test_bad_source_identifiers_rejected(key, kind, value):
    config = deepcopy(CONFIG)
    config["source"][key] = value
    with pytest.raises(ValueError, match="database.table"):
        capture_query(config, kind)
