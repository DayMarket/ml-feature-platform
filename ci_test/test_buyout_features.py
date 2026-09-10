"""Контрактные проверки группы витрин модели невыкупов (MAD-13413).

Реестр ENTITIES держит все семь сущностей группы `buyout-features`. Сущности,
которых ещё нет на диске, пропускаются с сообщением: части группы делаются
параллельно, но реестр остаётся полным.
"""

import ast
import importlib.util
import re
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
ONCALL_WEBHOOK_CONN_ID = "team:buyer"
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
                "feature-platform.layers.gold.key_type_key_id."
                "buyout_item_signal_features",
            ),
        ),
    },
    "sku_buyout": {
        "layer": "gold",
        "group": "sku_id",
        "entity": "sku_buyout_features",
        "table": "iceberg.gold.feature_platform_sku_buyout_features",
        "primary_key": ("date", "sku_id"),
        "schedule": "0 7 * * *",
        "engine": "trino",
        "dq_sources": (
            (
                "online_sku",
                "feature-platform.layers.gold.sku_id."
                "buyout_online_sku_features",
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
                "feature-platform.layers.silver.city_id_dimensional_group."
                "delivery_cpi_city_features",
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
        "dq_sources": (
            (
                "account_lifetime_facts",
                "feature-platform.layers.silver.account_id."
                "account_lifetime_facts",
            ),
        ),
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
                "feature-platform.layers.gold.account_id."
                "buyout_account_history_features",
            ),
            (
                "order_completion_city",
                "feature-platform.layers.silver.order_city_id."
                "order_completion_city_features",
            ),
            (
                "order_completion_region",
                "feature-platform.layers.silver.order_region_id."
                "order_completion_region_features",
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


# Источники DQ вне группы `buyout-features`: их контракт таблицы и алертов принадлежит
# другой команде, реестр ENTITIES их не описывает — сверяется только dag id владельца.
FOREIGN_DQ_SOURCES = {
    "order_completion_city": ("silver", "order_city_id", "order_completion_city_features"),
    "order_completion_region": (
        "silver",
        "order_region_id",
        "order_completion_region_features",
    ),
}


ACCOUNT_HISTORY_JOB = "job/getting_buyout_account_history_features.py"


def sql_template(source: str, function: str) -> str:
    """Текст SQL-шаблона функции: подстановки f-строки заменены на «?»."""
    tree = ast.parse(source)
    node = next(
        n for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == function
    )
    returned = next(n for n in ast.walk(node) if isinstance(n, ast.Return)).value
    if isinstance(returned, ast.Constant):
        return returned.value
    return "".join(
        part.value if isinstance(part, ast.Constant) else "?"
        for part in returned.values
    )


def select_output_names(select_list: str) -> set[str]:
    """Имена колонок на выходе SELECT-списка: алиас `AS x` или хвост `t.x`."""
    cleaned = "\n".join(line.split("--")[0] for line in select_list.splitlines())
    items: list[str] = []
    depth = 0
    buffer: list[str] = []
    for char in cleaned:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            items.append("".join(buffer))
            buffer = []
        else:
            buffer.append(char)
    items.append("".join(buffer))

    names = set()
    for item in items:
        item = " ".join(item.split())
        if not item:
            continue
        alias = re.search(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)$", item, re.IGNORECASE)
        names.add(alias.group(1) if alias else item.rsplit(".", 1)[-1])
    return names


def entity_dir(name: str) -> Path:
    if name in FOREIGN_DQ_SOURCES:
        return LAYERS.joinpath(*FOREIGN_DQ_SOURCES[name], "v1")
    spec = ENTITIES[name]
    return LAYERS / spec["layer"] / spec["group"] / spec["entity"] / "v1"


def is_present(name: str) -> bool:
    return (entity_dir(name) / "config.yaml").is_file()


def present_entities() -> list[str]:
    return [name for name in ENTITIES if is_present(name)]


def read_config(name: str) -> dict:
    return yaml.safe_load((entity_dir(name) / "config.yaml").read_text(encoding="utf-8"))


def expected_dag_id(name: str) -> str:
    if name in FOREIGN_DQ_SOURCES:
        layer, group, entity = FOREIGN_DQ_SOURCES[name]
    else:
        spec = ENTITIES[name]
        layer, group, entity = spec["layer"], spec["group"], spec["entity"]
    return f"feature-platform.layers.{layer}.{group}.{entity}"


def external_task_sensors(dag_source: str) -> list[tuple[str | None, str | None]]:
    """(external_dag_id, external_task_id) каждого сенсора; константы модуля разворачиваются."""
    tree = ast.parse(dag_source)
    constants: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            value = node.value
            if isinstance(target, ast.Name) and isinstance(value, ast.Constant):
                if isinstance(value.value, str):
                    constants[target.id] = value.value

    def resolve(node: ast.expr | None) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            return constants.get(node.id)
        return None

    sensors = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        if name != "ExternalTaskSensor":
            continue
        keywords = {kw.arg: kw.value for kw in node.keywords}
        sensors.append(
            (
                resolve(keywords.get("external_dag_id")),
                resolve(keywords.get("external_task_id")),
            )
        )
    return sensors


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
        """Сенсор ждёт таску dq DAG'а-владельца, а не легаси dbt-DQ-DAG.

        У `dbt.source.trino.ml_feature_platform_*.dq` собственное расписание
        `0 1 * * *` и собственная логическая дата: `execution_delta`, посчитанная
        от расписания производителя, в неё не попадает и сенсор висит до таймаута.
        """
        for name in present_entities():
            spec = ENTITIES[name]
            if not spec["dq_sources"]:
                continue
            dag_source = (entity_dir(name) / "dag.py").read_text(encoding="utf-8")
            sensors = external_task_sensors(dag_source)
            with self.subTest(entity=name):
                self.assertEqual(len(sensors), len(spec["dq_sources"]))
                self.assertNotIn("dbt.source.trino.ml_feature_platform", dag_source)
            waited = {
                (upstream_dag_id, task_id) for upstream_dag_id, task_id in sensors
            }
            for source_name, expected_upstream in spec["dq_sources"]:
                with self.subTest(entity=name, source=source_name):
                    self.assertEqual(expected_dag_id(source_name), expected_upstream)
                    self.assertIn((expected_upstream, "dq"), waited)

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
        # MAD-13695: выкупаемость магазина стянута к маркетплейсу, общая выкупаемость отдаётся колонкой
        self.assertIn("CROSS JOIN global_rate g", sql)
        for column in ("shop_buyout_rate_shrunk_90d", "marketplace_buyout_rate_90d",
                       "marketplace_no_show_rate_90d"):
            with self.subTest(column=column):
                self.assertIn(column, sql)
        # MAD-13695: население — активные sku в наличии плюс sku с доставками; подстановки
        # категории и маркетплейса считаются здесь, а не в сервисе
        self.assertIn("status = 'ACTIVE'", sql)
        self.assertIn("OR id IN (SELECT key_id FROM sig WHERE key_type = 'sku')", sql)
        self.assertIn("LEFT JOIN sig s       ON s.key_type = 'sku'", sql)
        self.assertIn("COALESCE(c.cat_buyout_90d,  g.g_buyout)", sql)
        self.assertIn("COALESCE(s.n_delivered_90d, 0)                      AS sku_n_delivered_90d", sql)

    def test_online_sku_migrations_declare_shop_shrunk_columns(self):
        if not is_present("online_sku"):
            self.fail("Сущность online_sku отсутствует на диске")
        migrations = entity_dir("online_sku") / "migrations"
        create_sql = (migrations / "create_table.sql").read_text(encoding="utf-8")
        alter_sql = (migrations / "20260909_shop_shrunk_and_marketplace_rates.sql").read_text(
            encoding="utf-8"
        )
        for column in ("shop_buyout_rate_shrunk_90d", "marketplace_buyout_rate_90d",
                       "marketplace_no_show_rate_90d"):
            with self.subTest(column=column):
                self.assertIn(f"    {column} DOUBLE COMMENT", create_sql)
                self.assertIn(f"ADD COLUMN IF NOT EXISTS {column} DOUBLE COMMENT", alter_sql)

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


class BuyoutAccountHistoryQueryTest(unittest.TestCase):
    """Сборка витрины склеивает три временные вьюхи — контракт колонок между ними."""

    def setUp(self):
        if not is_present("account_history"):
            self.fail("Сущность account_history отсутствует на диске")
        self.source = (entity_dir("account_history") / ACCOUNT_HISTORY_JOB).read_text(
            encoding="utf-8"
        )

    def test_final_select_reads_only_columns_asof_history_projects(self):
        asof = sql_template(self.source, "asof_history_sql")
        final_select = re.search(r"\nSELECT\n(.*?)\nFROM agg AS a", asof, re.S)
        self.assertIsNotNone(
            final_select, "не найден финальный SELECT вьюхи asof_history"
        )
        produced = select_output_names(final_select.group(1))

        features = sql_template(self.source, "features_sql")
        consumed = set(re.findall(r"\bh\.([a-z_][a-z0-9_]*)", features))

        missing = sorted(consumed - produced)
        self.assertFalse(
            missing,
            "features_sql читает из asof_history колонки, которых нет в её проекции: "
            + ", ".join(missing),
        )

    def test_geo_join_keys_reach_the_target_table(self):
        """Ключи связи с гео-витринами обязаны дойти от agg до записи в таблицу."""
        asof = sql_template(self.source, "asof_history_sql")
        final_select = re.search(r"\nSELECT\n(.*?)\nFROM agg AS a", asof, re.S)
        produced = select_output_names(final_select.group(1))
        create_sql = (
            entity_dir("account_history") / "migrations" / "create_table.sql"
        ).read_text(encoding="utf-8")
        for column in ("last_order_city_id", "last_order_region_id"):
            with self.subTest(column=column):
                self.assertIn(column, produced)
                self.assertIn(f"    {column} BIGINT", create_sql)


if __name__ == "__main__":
    unittest.main()
