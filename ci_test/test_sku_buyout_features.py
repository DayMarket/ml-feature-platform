"""Контракт витрины экономики корзины и выкупаемости на грейне sku_id."""

import importlib.util
import unittest
from datetime import date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers" / "gold" / "sku_id" / "sku_buyout_features" / "v1"

# Колонки витрины в порядке миграции. 21 из них уезжают в PostgreSQL;
# category_id остаётся только в Iceberg как ключ джойна с dict.category.
EXPECTED_COLUMNS = (
    "date",
    "sku_id",
    "product_id",
    "seller_id",
    "category_id",
    "l1_category",
    "l2_category",
    "l3_category",
    "l4_category",
    "l5_category",
    "type",
    "commission",
    "cost_price",
    "is_not_block",
    "sku_buyout",
    "product_buyout",
    "category_buyout",
    "shop_buyout",
    "category_no_show",
    "sku_n_delivered",
    "product_n_delivered",
    "predicted_dimensional_group",
)


def read_config() -> dict:
    with (ENTITY / "config.yaml").open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def read_migration() -> str:
    return (ENTITY / "migrations" / "create_table.sql").read_text(encoding="utf-8")


class MigrationContract(unittest.TestCase):
    def test_migration_declares_every_expected_column(self):
        sql = read_migration()
        for column in EXPECTED_COLUMNS:
            with self.subTest(column=column):
                self.assertRegex(sql, rf"\n\s+{column}\s+[A-Z]")

    def test_every_column_carries_a_comment(self):
        sql = read_migration()
        self.assertEqual(
            sql.count("COMMENT '"),
            len(EXPECTED_COLUMNS) + 1,
            "каждая колонка плюс COMMENT самой таблицы",
        )

    def test_migration_disables_hive_lock(self):
        self.assertIn(
            "TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')",
            read_migration(),
        )

    def test_partitioned_by_date(self):
        self.assertIn("PARTITIONED BY (date)", read_migration())


class ConfigContract(unittest.TestCase):
    def test_table_identifier(self):
        config = read_config()
        self.assertEqual(config["table"]["catalog"], "iceberg")
        self.assertEqual(config["table"]["schema"], "gold")
        self.assertEqual(
            config["table"]["name"], "feature_platform_sku_buyout_features"
        )
        self.assertEqual(config["table"]["primary_key"], "date,sku_id")

    def test_shards_are_configured(self):
        self.assertEqual(read_config()["source"]["shards"], 8)

    def test_dq_and_feature_stats_agree_on_partition_template(self):
        config = read_config()
        self.assertEqual(
            config["dq"]["partition_date_template"],
            config["feature_stats"]["partition_date_template"],
        )

    def test_identifier_columns_are_excluded_from_feature_stats(self):
        excluded = set(read_config()["feature_stats"]["exclude_columns"])
        self.assertEqual(
            excluded,
            {
                "product_id",
                "seller_id",
                "category_id",
                "l1_category",
                "l2_category",
                "l3_category",
                "l4_category",
                "l5_category",
            },
        )


def load_query_module():
    path = ENTITY / "job" / "query.py"
    spec = importlib.util.spec_from_file_location("sku_buyout_features_query", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


SIGNAL_TABLE = '"dwh-iceberg".gold.feature_platform_buyout_online_sku_features'


class QueryContract(unittest.TestCase):
    def build(self, lower=None, upper=None):
        return load_query_module().build_query(
            date(2026, 9, 9), SIGNAL_TABLE, lower, upper
        )

    def test_pins_the_partition_date(self):
        self.assertIn("DATE '2026-09-09'", self.build())

    def test_never_unions_the_two_type_branches(self):
        # Ветка 3p не фильтрует продавца и содержит все 179 081 sku ветки 1p.
        # UNION задвоил бы их; тип определяется антиджойном.
        sql = self.build().upper()
        self.assertNotIn("UNION", sql)

    def test_type_is_decided_by_the_one_p_antijoin(self):
        sql = self.build()
        self.assertIn("WHEN one_p.sku_id IS NOT NULL THEN '1p'", sql)
        self.assertIn("ELSE '3p'", sql)

    def test_commission_is_null_for_one_p(self):
        self.assertIn("WHEN one_p.sku_id IS NULL THEN comm.commission", self.build())

    def test_uses_shrunk_rates_where_they_exist(self):
        sql = self.build()
        self.assertIn("sku_buyout_rate_shrunk_90d AS sku_buyout", sql)
        self.assertIn("product_buyout_rate_shrunk_90d AS product_buyout", sql)
        self.assertIn("shop_buyout_rate_shrunk_90d AS shop_buyout", sql)

    def test_category_rates_have_no_shrunk_variant(self):
        sql = self.build()
        self.assertIn("category_buyout_rate_90d AS category_buyout", sql)
        self.assertIn("category_no_show_rate_90d AS category_no_show", sql)
        self.assertNotIn("category_buyout_rate_shrunk_90d", sql)

    def test_no_coalesce_cascade_over_buyout_rates(self):
        # _shrunk-колонки уже содержат подстановку родителя, COALESCE был бы
        # вторым сглаживанием поверх первого.
        self.assertNotIn("COALESCE(sku_buyout", self.build())

    def test_category_cascade_starts_at_l1(self):
        # У 23 категорий l2_category = 0; каскад обязан падать до l1.
        sql = self.build()
        self.assertIn("NULLIF(c.l2_category, 0)", sql)
        self.assertIn("NULLIF(c.l5_category, 0)", sql)

    def test_shard_bounds_are_range_predicates_on_every_large_source(self):
        sql = self.build(lower=1000000, upper=2000000)
        self.assertIn("s.id >= 1000000", sql)
        self.assertIn("s.id < 2000000", sql)
        self.assertIn("ke.id >= 1000000", sql)
        self.assertIn("ke.id < 2000000", sql)
        self.assertIn("f.sku_id >= 1000000", sql)

    def test_open_ended_shards_omit_the_missing_bound(self):
        first = self.build(lower=None, upper=2000000)
        self.assertNotIn(">= None", first)
        last = self.build(lower=1000000, upper=None)
        self.assertNotIn("< None", last)

    def test_reads_only_the_requested_signal_partition(self):
        self.assertIn(f"FROM {SIGNAL_TABLE}", self.build())

    def test_is_not_block_is_a_constant(self):
        self.assertIn("false AS is_not_block", self.build())

    def test_select_emits_exactly_the_migration_columns_in_order(self):
        sql = self.build()
        select_clause = sql[sql.rindex("\nSELECT\n") : sql.index("\nFROM dims")]
        emitted = []
        for line in select_clause.splitlines():
            line = line.strip().rstrip(",")
            if not line or line == "SELECT":
                continue
            if " AS " in line:
                emitted.append(line.rsplit(" AS ", 1)[1].strip())
            else:
                emitted.append(line.rsplit(".", 1)[-1].strip())
        self.assertEqual(tuple(emitted), EXPECTED_COLUMNS)


if __name__ == "__main__":
    unittest.main()
