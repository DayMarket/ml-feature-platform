"""Контрактные проверки группы витрин модели невыкупов (MAD-13413).

Реестр ENTITIES держит все семь сущностей группы `buyout-features`. Сущности,
которых ещё нет на диске, пропускаются с сообщением: части группы делаются
параллельно, но реестр остаётся полным.
"""

import importlib.util
import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
LAYERS = ROOT / "layers"

GROUP_TAG = "buyout-features"
TEAM = "buyer"
TABLE_TEAM = "team:buyer"
DAG_OWNER = "team:buyer"
ALERT_SEVERITY = "P2"
ONCALL_WEBHOOK_CONN_ID = "oncall_webhook_buyer"
START_DATE = "2026-08-20T00:00:00Z"

# layer / group / entity + ожидаемый контракт таблицы и оркестрации.
ENTITIES = {
    "delivery_cpi_city": {
        "layer": "silver",
        "group": "city_id_dimensional_group",
        "entity": "delivery_cpi_city_features",
        "table": "iceberg.silver.feature_platform_delivery_cpi_city_features",
        "primary_key": ("date", "city_id", "dimensional_group"),
        "schedule": "0 3 * * *",
        "engine": "trino",
        "dq_sources": (),
    },
    "account_lifetime_facts": {
        "layer": "silver",
        "group": "account_id",
        "entity": "account_lifetime_facts",
        "table": "iceberg.silver.feature_platform_account_lifetime_facts",
        "primary_key": ("date", "account_id"),
        "schedule": "0 2 * * *",
        "engine": "clickhouse",
        "dq_sources": (),
    },
    "item_signal": {
        "layer": "gold",
        "group": "key_type_key_id",
        "entity": "buyout_item_signal_features",
        "table": "iceberg.gold.feature_platform_buyout_item_signal_features",
        "primary_key": ("date", "key_type", "key_id"),
        "schedule": "0 3 * * *",
        "engine": "trino",
        "dq_sources": (),
    },
    "online_sku": {
        "layer": "gold",
        "group": "sku_id",
        "entity": "buyout_online_sku_features",
        "table": "iceberg.gold.feature_platform_buyout_online_sku_features",
        "primary_key": ("date", "sku_id"),
        "schedule": "0 6 * * *",
        "engine": "trino",
        "dq_sources": (
            (
                "item_signal",
                "dbt.source.trino.ml_feature_platform_gold."
                "feature_platform_buyout_item_signal_features.dq",
            ),
        ),
    },
    "online_city": {
        "layer": "gold",
        "group": "city_id_dimensional_group",
        "entity": "buyout_online_city_features",
        "table": "iceberg.gold.feature_platform_buyout_online_city_features",
        "primary_key": ("date", "city_id", "dimensional_group"),
        "schedule": "0 6 * * *",
        "engine": "trino",
        "dq_sources": (
            (
                "delivery_cpi_city",
                "dbt.source.trino.ml_feature_platform_silver."
                "feature_platform_delivery_cpi_city_features.dq",
            ),
        ),
    },
    # Spark-контур аккаунтов и его online-проекция.
    "account_history": {
        "layer": "gold",
        "group": "account_id",
        "entity": "buyout_account_history_features",
        "table": "iceberg.gold.feature_platform_buyout_account_history_features",
        "primary_key": ("date", "account_id"),
        "schedule": "0 4 * * *",
        "engine": "spark",
        "dq_sources": (),
    },
    "online_account": {
        "layer": "gold",
        "group": "account_id",
        "entity": "buyout_online_account_features",
        "table": "iceberg.gold.feature_platform_buyout_online_account_features",
        "primary_key": ("date", "account_id"),
        "schedule": "0 6 * * *",
        "engine": "trino",
        "dq_sources": (
            (
                "account_history",
                "dbt.source.trino.ml_feature_platform_gold."
                "feature_platform_buyout_account_history_features.dq",
            ),
        ),
    },
}

# Имена колонок сигнала, в которых окно зашито явно.
ITEM_SIGNAL_WINDOW_COLUMNS = (
    "n_delivered_90d",
    "n_completed_90d",
    "n_nonbuyout_client_90d",
    "buyout_rate_items_90d",
    "buyout_rate_money_90d",
    "n_delivered_30d",
    "n_completed_30d",
    "buyout_rate_items_30d",
    "no_show_rate_30d",
)


def entity_dir(name: str) -> Path:
    spec = ENTITIES[name]
    return LAYERS / spec["layer"] / spec["group"] / spec["entity"] / "v1"


def is_present(name: str) -> bool:
    return (entity_dir(name) / "config.yaml").is_file()


def present_entities() -> list[str]:
    return [name for name in ENTITIES if is_present(name)]


def read_config(name: str) -> dict:
    return yaml.safe_load((entity_dir(name) / "config.yaml").read_text(encoding="utf-8"))


def expected_dag_id(name: str) -> str:
    spec = ENTITIES[name]
    return (
        f"feature-platform.layers.{spec['layer']}."
        f"{spec['group']}.{spec['entity']}"
    )


def dq_dag_id(config: dict) -> str:
    table = config["table"]
    return (
        f"dbt.source.trino.ml_feature_platform_{table['schema']}."
        f"{table['name']}.dq"
    )


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class FakeCatalog:
    def __init__(self, exists=True):
        self.exists = exists
        self.identifiers = []

    def table_exists(self, identifier):
        self.identifiers.append(("exists", identifier))
        return self.exists

    def load_table(self, identifier):
        self.identifiers.append(("load", identifier))
        return object()


class BuyoutRegistryTest(unittest.TestCase):
    def test_registry_reports_entities_not_on_disk(self):
        missing = [name for name in ENTITIES if not is_present(name)]
        self.assertFalse(
            missing,
            "В реестре есть сущности, отсутствующие на диске: "
            + ", ".join(sorted(missing)),
        )

    def test_at_least_one_entity_is_present(self):
        self.assertTrue(
            present_entities(),
            "Ни одной сущности группы buyout-features не найдено на диске",
        )


class BuyoutTableContractTest(unittest.TestCase):
    def test_table_identifiers_match_registry(self):
        for name in present_entities():
            with self.subTest(entity=name):
                spec = ENTITIES[name]
                table = read_config(name)["table"]
                self.assertEqual(table["catalog"], "iceberg")
                self.assertEqual(
                    f"{table['catalog']}.{table['schema']}.{table['name']}",
                    spec["table"],
                )
                self.assertEqual(
                    tuple(
                        column.strip()
                        for column in str(table["primary_key"]).split(",")
                    ),
                    spec["primary_key"],
                )

    def test_primary_key_columns_exist_in_migration(self):
        for name in present_entities():
            with self.subTest(entity=name):
                create_sql = (
                    entity_dir(name) / "migrations" / "create_table.sql"
                ).read_text(encoding="utf-8")
                self.assertIn("CREATE TABLE IF NOT EXISTS", create_sql)
                self.assertIn("'engine.hive.lock-enabled' = 'false'", create_sql)
                for column in ENTITIES[name]["primary_key"]:
                    self.assertIn(f"    {column} ", create_sql)

    def test_primary_key_group_matches_path(self):
        for name in present_entities():
            with self.subTest(entity=name):
                spec = ENTITIES[name]
                non_date = [
                    column for column in spec["primary_key"] if column != "date"
                ]
                self.assertEqual("_".join(non_date), spec["group"])


class BuyoutOwnershipTest(unittest.TestCase):
    def test_group_tag_team_and_alerts(self):
        for name in present_entities():
            with self.subTest(entity=name):
                config = read_config(name)
                self.assertEqual(config["table"]["meta"]["team"], TABLE_TEAM)
                self.assertEqual(config["dag"]["group_tag"], GROUP_TAG)
                self.assertEqual(config["dag"]["team"], TEAM)
                # У Spark-сущностей owner может опускаться: factory выводит его
                # из dag.team как team:<team>.
                self.assertEqual(
                    config["dag"].get("owner", f"team:{config['dag']['team']}"),
                    DAG_OWNER,
                )
                self.assertEqual(config["alerts"]["team"], TEAM)
                self.assertEqual(config["alerts"]["severity"], ALERT_SEVERITY)
                self.assertEqual(
                    config["alerts"]["oncall_webhook_conn_id"],
                    ONCALL_WEBHOOK_CONN_ID,
                )


class BuyoutOrchestrationTest(unittest.TestCase):
    def dag_source(self, name: str) -> str:
        dag_path = entity_dir(name) / "dag.py"
        self.assertTrue(dag_path.is_file(), f"{dag_path} отсутствует")
        return dag_path.read_text(encoding="utf-8")

    def test_dag_id_matches_repository_path(self):
        for name in present_entities():
            with self.subTest(entity=name):
                contract = (entity_dir(name) / "config.yaml").read_text(
                    encoding="utf-8"
                ) + self.dag_source(name)
                self.assertIn(expected_dag_id(name), contract)
                config = read_config(name)
                if "id" in config["dag"]:
                    self.assertEqual(config["dag"]["id"], expected_dag_id(name))

    def test_schedule_and_start_date(self):
        for name in present_entities():
            with self.subTest(entity=name):
                spec = ENTITIES[name]
                config = read_config(name)
                dag_source = self.dag_source(name)

                if "schedule" in config["dag"]:
                    self.assertEqual(config["dag"]["schedule"], spec["schedule"])
                else:
                    # Spark-сущность может задавать cron прямо в dag.py.
                    self.assertIn(spec["schedule"], dag_source)

                if "start_date" in config["dag"]:
                    start_date = config["dag"]["start_date"]
                    # Кавычки должны сниматься yaml, иначе pendulum.parse упадёт.
                    self.assertEqual(start_date, START_DATE)
                    self.assertEqual(
                        datetime.fromisoformat(start_date.replace("Z", "+00:00")),
                        datetime(2026, 8, 20, tzinfo=timezone.utc),
                    )
                else:
                    self.assertIn(START_DATE[:10], dag_source)

    def test_dag_schedules_in_utc(self):
        for name in present_entities():
            with self.subTest(entity=name):
                dag_source = self.dag_source(name)
                if ENTITIES[name]["engine"] == "spark":
                    # Spark-DAG может передавать таймзону позиционно.
                    self.assertIn("UTC", dag_source)
                else:
                    self.assertIn('timezone="UTC"', dag_source)
                    self.assertIn('CONFIG["dag"]["id"]', dag_source)


class BuyoutSensorTest(unittest.TestCase):
    def test_declared_dq_sensors_match_source_configs(self):
        for name in present_entities():
            spec = ENTITIES[name]
            if not spec["dq_sources"]:
                continue
            dag_source = (entity_dir(name) / "dag.py").read_text(encoding="utf-8")
            with self.subTest(entity=name):
                self.assertIn("ExternalTaskSensor", dag_source)
            for source_name, expected_dq in spec["dq_sources"]:
                with self.subTest(entity=name, source=source_name):
                    self.assertIn(ENTITIES[source_name]["entity"], dag_source)
                    if not is_present(source_name):
                        self.skipTest(
                            f"Источник {source_name} ещё не создан: "
                            "сверка DQ id по config.yaml пропущена"
                        )
                    self.assertEqual(dq_dag_id(read_config(source_name)), expected_dq)

    def test_entities_without_dependencies_declare_no_sensor(self):
        for name in present_entities():
            if ENTITIES[name]["dq_sources"] or ENTITIES[name]["engine"] == "spark":
                continue
            with self.subTest(entity=name):
                dag_source = (entity_dir(name) / "dag.py").read_text(encoding="utf-8")
                self.assertNotIn("ExternalTaskSensor", dag_source)


class BuyoutRuntimeContractTest(unittest.TestCase):
    """PyIceberg-идентификатор — строго (schema, table) из config.yaml."""

    def runtime_entities(self) -> list[str]:
        return [
            name
            for name in present_entities()
            if ENTITIES[name]["engine"] in ("trino", "clickhouse")
            and (entity_dir(name) / "job" / "runtime.py").is_file()
        ]

    def test_identifier_is_schema_and_table_only(self):
        for name in self.runtime_entities():
            with self.subTest(entity=name):
                runtime = load_module(
                    entity_dir(name) / "job" / "runtime.py",
                    f"buyout_{name}_runtime_identifier",
                )
                config = runtime.load_config(entity_dir(name) / "config.yaml")
                ref = runtime.table_ref(config)
                catalog = FakeCatalog()
                runtime.preflight_table(catalog, ref)
                self.assertEqual(
                    catalog.identifiers,
                    [
                        ("exists", (config["table"]["schema"], config["table"]["name"])),
                        ("load", (config["table"]["schema"], config["table"]["name"])),
                    ],
                )

    def test_malformed_identifiers_are_rejected(self):
        for name in self.runtime_entities():
            with self.subTest(entity=name):
                runtime = load_module(
                    entity_dir(name) / "job" / "runtime.py",
                    f"buyout_{name}_runtime_malformed",
                )
                for table in (
                    {"catalog": "iceberg", "schema": "silver.table", "name": "table"},
                    {"catalog": "iceberg", "schema": "silver", "name": "silver.table"},
                    {"catalog": "iceberg", "schema": "", "name": "table"},
                ):
                    with self.assertRaises(ValueError):
                        runtime.table_ref({"table": table})

    def test_missing_table_reports_migration_contract(self):
        for name in self.runtime_entities():
            with self.subTest(entity=name):
                runtime = load_module(
                    entity_dir(name) / "job" / "runtime.py",
                    f"buyout_{name}_runtime_missing",
                )
                config = runtime.load_config(entity_dir(name) / "config.yaml")
                ref = runtime.table_ref(config)
                with self.assertRaises(RuntimeError) as raised:
                    runtime.preflight_table(FakeCatalog(exists=False), ref)
                self.assertIn("migrations", str(raised.exception))


class BuyoutItemSignalQueryTest(unittest.TestCase):
    def setUp(self):
        if not is_present("item_signal"):
            self.fail("Сущность item_signal отсутствует на диске")
        self.query = load_module(
            entity_dir("item_signal") / "job" / "query.py",
            "buyout_item_signal_query_under_test",
        )

    def test_query_pins_snapshot_source_and_windows(self):
        sql = self.query.build_query(date(2026, 8, 1))
        self.assertIn('"dwh-iceberg".silver.history_order_items', sql)
        self.assertIn('"dwh-iceberg".silver.sku', sql)
        self.assertIn("analyze_date = DATE '2026-08-01'", sql)
        self.assertIn("date_add('day', -90, DATE '2026-08-01')", sql)
        self.assertIn("date_add('day', -30, DATE '2026-08-01')", sql)
        self.assertIn("WHERE key_id IS NOT NULL", sql)
        for column in ITEM_SIGNAL_WINDOW_COLUMNS:
            self.assertIn(column, sql)

    def test_query_excludes_active_and_keeps_grouping_sets(self):
        sql = self.query.build_query(date(2026, 8, 1))
        self.assertIn("real_order_item_status <> 'ACTIVE'", sql)
        self.assertIn(
            "GROUP BY GROUPING SETS "
            "((sku_id), (product_id), (category_id), (shop_id), (brand_name_id))",
            sql,
        )

    def test_migration_declares_every_selected_window_column(self):
        create_sql = (
            entity_dir("item_signal") / "migrations" / "create_table.sql"
        ).read_text(encoding="utf-8")
        for column in ITEM_SIGNAL_WINDOW_COLUMNS:
            self.assertIn(f"    {column} ", create_sql)


class BuyoutProjectionQueryTest(unittest.TestCase):
    """Online-проекции читают партицию источника, а не пересобирают семантику."""

    def test_online_sku_projection_reads_signal_partition(self):
        if not is_present("online_sku"):
            self.fail("Сущность online_sku отсутствует на диске")
        query = load_module(
            entity_dir("online_sku") / "job" / "query.py",
            "buyout_online_sku_query_under_test",
        )
        signal_table = '"dwh-iceberg".gold.feature_platform_buyout_item_signal_features'
        sql = query.build_query(date(2026, 8, 1), signal_table)
        self.assertIn(signal_table, sql)
        self.assertIn("WHERE date = DATE '2026-08-01'", sql)
        self.assertIn("sku_vs_product_gap_90d", sql)
        self.assertIn("sku_buyout_rate_shrunk_90d", sql)

    def test_online_city_projection_reads_silver_partition(self):
        if not is_present("online_city"):
            self.fail("Сущность online_city отсутствует на диске")
        query = load_module(
            entity_dir("online_city") / "job" / "query.py",
            "buyout_online_city_query_under_test",
        )
        source_table = (
            '"dwh-iceberg".silver.feature_platform_delivery_cpi_city_features'
        )
        sql = query.build_query(date(2026, 8, 1), source_table)
        self.assertIn(source_table, sql)
        self.assertIn("WHERE date = DATE '2026-08-01'", sql)
        self.assertIn("dimensional_group", sql)
        self.assertIn("cpi_forward_country_uzs", sql)


if __name__ == "__main__":
    unittest.main()
