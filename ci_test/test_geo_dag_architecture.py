import importlib.util
import re
import sys
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
SILVER_ENTITIES = (
    "dp_neighbor_order_features",
    "geo_geointellect_features",
    "geo_user_activity_features",
    "geo_user_location_features",
    "geo_yandex_poi_features",
)
GOLD_ENTITY = "location_h3_forecast_features"
PRIMARY_KEY_GROUP = "h3_index"
GROUP_TAG = "location-h3-forecast"


def load_runtime(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class FakeCatalog:
    def __init__(self):
        self.identifiers = []
        self.table = object()

    def table_exists(self, identifier):
        self.identifiers.append(("exists", identifier))
        return True

    def load_table(self, identifier):
        self.identifiers.append(("load", identifier))
        return self.table


class GeoDagArchitectureTest(unittest.TestCase):
    def test_each_entity_has_own_dag_and_documented_contract(self):
        entities = [
            ("silver", entity) for entity in SILVER_ENTITIES
        ] + [("gold", GOLD_ENTITY)]

        for layer, entity in entities:
            with self.subTest(layer=layer, entity=entity):
                entity_dir = (
                    ROOT / "layers" / layer / PRIMARY_KEY_GROUP / entity / "v1"
                )
                config = yaml.safe_load(
                    (entity_dir / "config.yaml").read_text(encoding="utf-8")
                )
                expected_dag_id = (
                    f"feature-platform.layers.{layer}.{PRIMARY_KEY_GROUP}.{entity}"
                )
                expected_table = (
                    f"{config['table']['catalog']}."
                    f"{config['table']['schema']}."
                    f"{config['table']['name']}"
                )
                readme = (entity_dir / "README.md").read_text(encoding="utf-8")

                self.assertTrue((entity_dir / "dag.py").is_file())
                self.assertTrue((entity_dir / "job" / "runtime.py").is_file())
                self.assertEqual(config["dag"]["id"], expected_dag_id)
                self.assertEqual(config["dag"]["group_tag"], GROUP_TAG)
                self.assertIsNotNone(
                    re.fullmatch(r"[A-Za-z0-9_.-]+", config["dag"]["id"]),
                    "DAG id must satisfy Airflow validate_key",
                )
                self.assertIn(expected_dag_id, readme)
                self.assertIn(expected_table, readme)
                self.assertIn(GROUP_TAG, readme)
                dag_source = (entity_dir / "dag.py").read_text(encoding="utf-8")
                self.assertIn('CONFIG["dag"]["group_tag"]', dag_source)

    def test_group_tag_is_not_reused_outside_location_forecast_group(self):
        expected_configs = {
            ROOT
            / "layers"
            / "silver"
            / PRIMARY_KEY_GROUP
            / entity
            / "v1"
            / "config.yaml"
            for entity in SILVER_ENTITIES
        }
        expected_configs.add(
            ROOT
            / "layers"
            / "gold"
            / PRIMARY_KEY_GROUP
            / GOLD_ENTITY
            / "v1"
            / "config.yaml"
        )
        actual_configs = set()
        for config_path in (ROOT / "layers").glob("**/config.yaml"):
            config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            if config.get("dag", {}).get("group_tag") == GROUP_TAG:
                actual_configs.add(config_path)

        self.assertEqual(actual_configs, expected_configs)

    def test_forbidden_layers_common_is_not_tracked(self):
        tracked_common = [
            path
            for path in (ROOT / "layers").glob("_common*/**/*")
            if path.is_file()
        ]
        self.assertEqual(tracked_common, [])

    def test_pyiceberg_identifier_is_two_part_tuple_from_config(self):
        for layer, entity in [
            ("silver", SILVER_ENTITIES[0]),
            ("gold", GOLD_ENTITY),
        ]:
            with self.subTest(layer=layer, entity=entity):
                runtime_path = (
                    ROOT
                    / "layers"
                    / layer
                    / PRIMARY_KEY_GROUP
                    / entity
                    / "v1"
                    / "job"
                    / "runtime.py"
                )
                runtime = load_runtime(
                    runtime_path,
                    f"test_{layer}_{entity}_runtime",
                )
                config = {
                    "table": {
                        "catalog": "iceberg",
                        "schema": layer,
                        "name": f"feature_platform_{entity}",
                    }
                }
                ref = runtime.table_ref(config)
                catalog = FakeCatalog()

                resolved = runtime.preflight_table(catalog, ref)

                self.assertIs(resolved, catalog.table)
                self.assertEqual(
                    catalog.identifiers,
                    [
                        ("exists", (layer, f"feature_platform_{entity}")),
                        ("load", (layer, f"feature_platform_{entity}")),
                    ],
                )

    def test_pyiceberg_identifier_rejects_embedded_paths(self):
        runtime = load_runtime(
            ROOT
            / "layers"
            / "silver"
            / PRIMARY_KEY_GROUP
            / SILVER_ENTITIES[0]
            / "v1"
            / "job"
            / "runtime.py",
            "test_invalid_geo_runtime",
        )
        for schema, name in [
            ("iceberg.silver", "table"),
            ("silver", "namespace.table"),
        ]:
            with self.subTest(schema=schema, name=name):
                with self.assertRaisesRegex(ValueError, "separate schema"):
                    runtime.table_ref(
                        {
                            "table": {
                                "catalog": "iceberg",
                                "schema": schema,
                                "name": name,
                            }
                        }
                    )

    def test_gold_has_one_dq_sensor_per_silver_entity(self):
        gold_dir = (
            ROOT / "layers" / "gold" / PRIMARY_KEY_GROUP / GOLD_ENTITY / "v1"
        )
        dag_source = (gold_dir / "dag.py").read_text(encoding="utf-8")
        config = yaml.safe_load(
            (gold_dir / "config.yaml").read_text(encoding="utf-8")
        )

        self.assertIn("ExternalTaskSensor", dag_source)
        self.assertIn("silver_dq_sensors >> gold_task", dag_source)
        self.assertEqual(config["dag"]["schedule"], "0 2 * * *")
        self.assertIn("execution_delta=timedelta(hours=1)", dag_source)
        for entity in SILVER_ENTITIES:
            self.assertIn(entity, dag_source)


if __name__ == "__main__":
    unittest.main()
