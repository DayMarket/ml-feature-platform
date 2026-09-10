"""Контракт витрины экономики корзины и выкупаемости на грейне sku_id."""

import unittest
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


if __name__ == "__main__":
    unittest.main()
