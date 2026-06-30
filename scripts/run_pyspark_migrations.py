import argparse
import os
import re
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession


COMMENT_LINE_PATTERN = re.compile(r"^\s*--")
ADD_COLUMN_IF_NOT_EXISTS_PATTERN = re.compile(
    r"^\s*ALTER\s+TABLE\s+(?P<table>\S+)\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+"
    r"(?P<column>`?[\w]+`?)\s+(?P<definition>.+?)\s*$",
    re.IGNORECASE | re.DOTALL,
)
RENAME_COLUMN_IF_EXISTS_PATTERN = re.compile(
    r"^\s*ALTER\s+TABLE\s+(?P<table>\S+)\s+RENAME\s+COLUMN\s+IF\s+EXISTS\s+"
    r"(?P<column>`?[\w]+`?)\s+TO\s+(?P<new_column>`?[\w]+`?)"
    r"(?:\s+WHEN\s+SOURCE\s+TYPE\s+IS\s+NOT\s+(?P<expected_type>.+?))?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SQL migrations through PySpark")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--config-path")
    parser.add_argument("--migrations-path")
    parser.add_argument(
        "--validation-mode",
        action="store_true",
        help="Run migrations against a disposable local Spark/Iceberg warehouse.",
    )
    parser.add_argument(
        "--validation-warehouse",
        default="/tmp/feature-platform-migration-warehouse",
        help="Local warehouse path used with --validation-mode.",
    )
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
    if normalized.startswith("ALTER TABLE") and " RENAME COLUMN " in normalized:
        if " RENAME COLUMN IF EXISTS " not in normalized:
            raise RuntimeError(
                f"{migration_path}: RENAME COLUMN migration must use IF EXISTS"
            )
    if normalized.startswith(("DROP ", "DELETE ", "TRUNCATE ")):
        raise RuntimeError(f"{migration_path}: destructive migrations are not allowed")


def get_layer_migration_targets(repo_root: Path) -> list[tuple[Path, Path]]:
    targets = []
    for config_root in ("layers", "datasets"):
        for config_path in sorted(repo_root.glob(f"{config_root}/**/config.yaml")):
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


def build_validation_spark_session(validation_warehouse: str) -> SparkSession:
    warehouse_path = Path(validation_warehouse).resolve()
    warehouse_path.mkdir(parents=True, exist_ok=True)

    return (
        SparkSession.builder.appName("feature-platform-pyspark-migration-validation")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", warehouse_path.as_uri())
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def build_spark_session(validation_mode: bool, validation_warehouse: str) -> SparkSession:
    if validation_mode:
        return build_validation_spark_session(validation_warehouse)

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


def normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip("`").lower()


def normalize_type(type_name: str) -> str:
    return "".join(type_name.strip().strip(";").lower().split())


def get_existing_columns(spark: SparkSession, table_name: str) -> set[str]:
    return {
        normalize_identifier(field.name)
        for field in spark.table(table_name).schema.fields
    }


def get_existing_column_types(spark: SparkSession, table_name: str) -> dict[str, str]:
    return {
        normalize_identifier(field.name): normalize_type(field.dataType.simpleString())
        for field in spark.table(table_name).schema.fields
    }


def run_statement(spark: SparkSession, statement: str) -> None:
    rename_column_match = RENAME_COLUMN_IF_EXISTS_PATTERN.match(statement)
    if rename_column_match:
        table_name = rename_column_match.group("table")
        column_name = normalize_identifier(rename_column_match.group("column"))
        new_column_name = normalize_identifier(rename_column_match.group("new_column"))
        column_types = get_existing_column_types(spark, table_name)

        if column_name not in column_types:
            print(f"Skip missing column {table_name}.{column_name}")
            return
        if new_column_name in column_types:
            print(f"Skip existing column {table_name}.{new_column_name}")
            return

        expected_type = rename_column_match.group("expected_type")
        if expected_type and column_types[column_name] == normalize_type(expected_type):
            print(
                f"Skip column {table_name}.{column_name}: "
                f"type already {column_types[column_name]}"
            )
            return

        spark.sql(
            f"ALTER TABLE {table_name} RENAME COLUMN "
            f"{rename_column_match.group('column')} TO "
            f"{rename_column_match.group('new_column')}"
        )
        return

    add_column_match = ADD_COLUMN_IF_NOT_EXISTS_PATTERN.match(statement)
    if not add_column_match:
        spark.sql(statement)
        return

    table_name = add_column_match.group("table")
    column_name = normalize_identifier(add_column_match.group("column"))
    if column_name in get_existing_columns(spark, table_name):
        print(f"Skip existing column {table_name}.{column_name}")
        return

    spark_statement = (
        f"ALTER TABLE {table_name} ADD COLUMN "
        f"{add_column_match.group('column')} {add_column_match.group('definition')}"
    )
    spark.sql(spark_statement)


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
            run_statement(spark, statement)


def run_migrations(
    spark: SparkSession,
    repo_root: Path,
    config_path: Path | None = None,
    migrations_path: Path | None = None,
) -> None:
    targets = get_migration_targets(repo_root, config_path, migrations_path)
    create_target_namespaces(spark, targets)

    for target_config_path, target_migrations_path in targets:
        run_layer_migrations(spark, target_config_path, target_migrations_path)


def get_migration_targets(
    repo_root: Path,
    config_path: Path | None = None,
    migrations_path: Path | None = None,
) -> list[tuple[Path, Path]]:
    if config_path or migrations_path:
        if not config_path or not migrations_path:
            raise RuntimeError("--config-path and --migrations-path must be passed together")
        targets = [(config_path, migrations_path)]
    else:
        targets = get_layer_migration_targets(repo_root)

    if not targets:
        raise RuntimeError(f"No migration targets found in {repo_root}")

    return targets


def create_target_namespaces(
    spark: SparkSession,
    targets: list[tuple[Path, Path]],
) -> None:
    namespaces = set()
    for config_path, _ in targets:
        config = read_simple_nested_config(config_path)
        table_config = config["table"]
        namespaces.add(f"{table_config['catalog']}.{table_config['schema']}")

    for namespace in sorted(namespaces):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def main() -> int:
    args = parse_args()
    spark = build_spark_session(
        validation_mode=args.validation_mode,
        validation_warehouse=args.validation_warehouse,
    )
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
