import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_shared_spark_profiles_define_cpu_requests():
    resources = json.loads((ROOT / "config/spark/resources.yaml").read_text())
    profiles = resources["profiles"]

    assert set(profiles) >= {"small", "medium", "large"}

    for profile_name, profile in profiles.items():
        assert "maxExecutors" not in profile
        assert "executor_core_request" in profile
        assert int(profile["executor_core_request"]) <= int(profile["executor_cores"])


def test_local_spark_resources_define_cpu_requests():
    resource_paths = sorted((ROOT / "layers").glob("**/config/resources.yaml"))
    resource_paths.extend(sorted((ROOT / "datasets").glob("**/config/resources.yaml")))
    resource_paths.extend(sorted((ROOT / "upload").glob("**/config/resources.yaml")))

    for resource_path in resource_paths:
        resources = json.loads(resource_path.read_text())
        if "executor_cores" not in resources:
            continue

        assert "maxExecutors" not in resources, resource_path
        assert "executor_core_request" in resources, resource_path
        assert int(resources["executor_core_request"]) <= int(
            resources["executor_cores"]
        )


def test_spark_application_templates_use_executor_core_request():
    template_paths = [ROOT / "config/spark/layer_spark_application.yaml"]
    template_paths.extend(sorted((ROOT / "layers").glob("**/config/*.yaml")))
    template_paths.extend(sorted((ROOT / "upload").glob("**/config/*.yaml")))

    spark_templates = [
        path
        for path in template_paths
        if "kind: SparkApplication" in path.read_text()
        and "<executor_cores>" in path.read_text()
    ]

    assert spark_templates

    for template_path in spark_templates:
        assert (
            'coreRequest: "<executor_core_request>"' in template_path.read_text()
        ), template_path


def test_spark_factories_fill_executor_core_request():
    factory_paths = sorted((ROOT / "layers").glob("**/config/factory.py"))
    factory_paths.extend(sorted((ROOT / "datasets").glob("**/config/factory.py")))
    factory_paths.extend(sorted((ROOT / "upload").glob("**/config/factory.py")))

    spark_factories = [
        path
        for path in factory_paths
        if '"<executor_cores>": str(' in path.read_text()
    ]

    assert spark_factories

    for factory_path in spark_factories:
        factory_content = factory_path.read_text()
        assert '"<executor_core_request>": str(' in factory_content, factory_path
        assert '.get("executor_core_request"' in factory_content, factory_path
