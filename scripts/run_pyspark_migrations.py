import argparse
import os
import re
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession


COMMENT_LINE_PATTERN = re.compile(r"^\s*--")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SQL migrations through PySpark")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--config-path")
    parser.add_argument("--migrations-path")
    return parser.parse_args()


def read_simple_nested_config(config_path: Path) -> dict[str, Any]:
    config: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, config)]
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip()
        key, separator, value = line.partition(":")
        if not separator or not key:
            continue

        while stack and indent <= stack[-1][0]:
            stack.pop()

        parent = stack[-1][1]
        key = key.strip()
        value = value.strip()
        if value:
            parent[key] = value
        else:
            nested: dict[str, Any] = {}
            parent[key] = nested
            stack.append((indent, nested))
    return config


def get_target_table(config: dict[str, Any]) -> str:
    table_config = config["table"]
    catalog = str(table_config["catalog"])
    schema = str(table_config["schema"])
    table_name = str(table_config["name"])
    return f"{catalog}.{schema}.{table_name}"


def split_sql(sql: str) -> list[str]:
    statements = []
    current_lines = []
    for line in sql.splitlines():
        if COMMENT_LINE_PATTERN.match(line):
            continue
        current_lines.append(line)
        if line.rstrip().endswith(";"):
            statement = "\n".join(current_lines).strip().rstrip(";").strip()
            if statement:
                statements.append(statement)
            current_lines = []

    tail_statement = "\n".join(current_lines).strip()
    if tail_statement:
        statements.append(tail_statement)
    return statements


def validate_idempotent_statement(statement: str, migration_path: Path) -> None:
    normalized = " ".join(statement.split()).upper()
    if normalized.startswith("CREATE TABLE") and not normalized.startswith(
        "CREATE TABLE IF NOT EXISTS"
    ):
        raise RuntimeError(
            f"{migration_path}: CREATE TABLE migration must use IF NOT EXISTS"
        )
    if normalized.startswith("ALTER TABLE") and " ADD COLUMN " in normalized:
        if " ADD COLUMN IF NOT EXISTS " not in normalized:
            raise RuntimeError(
                f"{migration_path}: ADD COLUMN migration must use IF NOT EXISTS"
            )
    if normalized.startswith(("DROP ", "DELETE ", "TRUNCATE ")):
        raise RuntimeError(f"{migration_path}: destructive migrations are not allowed")


def get_layer_migration_targets(repo_root: Path) -> list[tuple[Path, Path]]:
    targets = []
    for config_path in sorted(repo_root.glob("layers/**/config.yaml")):
        migrations_path = config_path.parent / "migrations"
        if migrations_path.is_dir() and list(migrations_path.glob("*.sql")):
            targets.append((config_path, migrations_path))
    return targets


def sort_migration_files(migrations_path: Path) -> list[Path]:
    return sorted(
        migrations_path.glob("*.sql"),
        key=lambda path: (path.name != "create_table.sql", path.name),
    )


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


def build_spark_session() -> SparkSession:
    s3_access_key = get_required_env("S3_ACCESS_KEY")
    s3_secret_key = get_required_env("S3_SECRET_KEY")
    hive_metastore_uris = get_required_env("HIVE_METASTORE_URIS")
    iceberg_warehouse = os.getenv(
        "ICEBERG_WAREHOUSE",
        "s3a://um-prod-data-platform-landing-layer/",
    )
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://storage.yandexcloud.net")
    aws_region = os.getenv("AWS_REGION", "ru-central1")

    return (
        SparkSession.builder.appName("feature-platform-pyspark-migrations")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.iceberg.uri", hive_metastore_uris)
        .config("spark.sql.catalog.iceberg.warehouse", iceberg_warehouse)
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.iceberg.s3.access-key-id", s3_access_key)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", s3_secret_key)
        .config("spark.sql.catalog.iceberg.s3.region", aws_region)
        .config("spark.sql.catalog.iceberg.client.region", aws_region)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net")
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.hive.metastore.uris", hive_metastore_uris)
        .enableHiveSupport()
        .getOrCreate()
    )


def run_layer_migrations(
    spark: SparkSession,
    config_path: Path,
    migrations_path: Path,
) -> None:
    config = read_simple_nested_config(config_path)
    target_table = get_target_table(config)
    migration_files = sort_migration_files(migrations_path)
    if not migration_files:
        raise RuntimeError(f"No SQL migrations found in {migrations_path}")

    for migration_path in migration_files:
        sql = migration_path.read_text(encoding="utf-8").format(
            target_table=target_table
        )
        statements = split_sql(sql)
        for statement in statements:
            validate_idempotent_statement(statement, migration_path)

        print(f"Running {migration_path} for {target_table} ({len(statements)} statements)")
        for statement in statements:
            spark.sql(statement)


def run_migrations(
    spark: SparkSession,
    repo_root: Path,
    config_path: Path | None = None,
    migrations_path: Path | None = None,
) -> None:
    if config_path or migrations_path:
        if not config_path or not migrations_path:
            raise RuntimeError("--config-path and --migrations-path must be passed together")
        targets = [(config_path, migrations_path)]
    else:
        targets = get_layer_migration_targets(repo_root)

    if not targets:
        raise RuntimeError(f"No migration targets found in {repo_root}")

    for target_config_path, target_migrations_path in targets:
        run_layer_migrations(spark, target_config_path, target_migrations_path)


def main() -> int:
    args = parse_args()
    spark = build_spark_session()
    try:
        run_migrations(
            spark=spark,
            repo_root=Path(args.repo_root),
            config_path=Path(args.config_path) if args.config_path else None,
            migrations_path=Path(args.migrations_path) if args.migrations_path else None,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
