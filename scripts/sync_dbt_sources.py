import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


TABLE_NAME_PATTERN = re.compile(r"^\s*-\s+name:\s+(?P<name>[A-Za-z0-9_]+)\s*$", re.MULTILINE)


@dataclass(frozen=True)
class RuntimeConfig:
    branch: str
    dbt_repo_url: str
    git_token: str
    workspace: Path
    dry_run: bool
    build_url: str


def main() -> int:
    repo_root = Path.cwd()
    _log_section("load config")
    print(f"repo_root={repo_root}")
    config = json.loads((repo_root / "config.yaml").read_text(encoding="utf-8"))
    runtime = RuntimeConfig(
        branch=os.getenv("DRONE_COMMIT_BRANCH", "local"),
        dbt_repo_url=_required_env(config["dbt"].get("repo_url_env", "DBT_REPO_URL")),
        git_token=_required_env("GIT_TOKEN"),
        workspace=repo_root / ".tmp_dbt_repo",
        dry_run=os.getenv("DRY_RUN", "false").lower() == "true",
        build_url=os.getenv("DRONE_BUILD_LINK", ""),
    )
    print(f"branch={runtime.branch}")
    print(f"dry_run={runtime.dry_run}")
    print(f"dbt_repo_url={_mask_token(runtime.dbt_repo_url)}")
    print(f"workspace={runtime.workspace}")
    print(f"models_path={config['dbt']['models_path']}")
    print(f"configured_tables={len(config['tables'])}")
    print(f"dq_enabled_branches={config['dbt'].get('dq_enabled_branches', [])}")

    if runtime.workspace.exists():
        print(f"remove_existing_workspace={runtime.workspace}")
        shutil.rmtree(runtime.workspace)

    _log_section("clone dbt repo")
    clone_url = _with_token(runtime.dbt_repo_url, runtime.git_token)
    _run(["git", "clone", clone_url, runtime.workspace.as_posix()])

    _log_section("checkout automation branch")
    target_branch = _checkout_branch(runtime, config["dbt"]["base_branch"])
    print(f"target_branch={target_branch}")
    models_path = runtime.workspace / config["dbt"]["models_path"]
    models_path.mkdir(parents=True, exist_ok=True)
    print(f"models_path_exists={models_path.exists()} path={models_path}")

    _log_section("scan existing dbt source tables")
    existing_tables = _existing_tables(models_path)
    print(f"existing_yaml_files={len(_yaml_files(models_path))}")
    print(f"existing_tables_count={len(existing_tables)}")
    if existing_tables:
        print("existing_tables=" + ", ".join(sorted(existing_tables)))

    created_files = []
    _log_section("render missing sources")
    for table_config in config["tables"]:
        table_name = str(table_config["name"])
        source_schema = _source_schema(config["dbt"], table_config, runtime.branch)
        include_dq = runtime.branch in set(config["dbt"].get("dq_enabled_branches", []))
        print(
            f"table={table_name} schema={source_schema} "
            f"include_dq={include_dq} primary_key={table_config['primary_key']}"
        )
        if table_name in existing_tables:
            print(f"Skip existing dbt source table: {table_name}")
            continue

        source_yaml = render_source_yaml(config["dbt"], table_config, runtime.branch)
        target_path = models_path / f"{table_name}.yml"
        target_path.write_text(source_yaml, encoding="utf-8")
        created_files.append(target_path)
        print(f"Added dbt source table: {table_name} -> {target_path.relative_to(runtime.workspace)}")
        print("rendered_yaml_begin")
        print(source_yaml.rstrip())
        print("rendered_yaml_end")

    if not created_files:
        _log_section("done")
        print("No dbt source changes to publish")
        return 0

    if runtime.dry_run:
        _log_section("dry run")
        _run(["git", "status", "--short"], cwd=runtime.workspace)
        print("DRY_RUN=true, skip commit/push/PR")
        return 0

    _log_section("publish changes")
    _publish_changes(runtime, target_branch, config["dbt"]["base_branch"], created_files)
    return 0


def render_source_yaml(
    dbt_config: dict[str, Any],
    table_config: dict[str, Any],
    branch: str,
) -> str:
    source_schema = _source_schema(dbt_config, table_config, branch)
    source_name = f"ml_feature_platform_{source_schema}"
    include_dq = branch in set(dbt_config.get("dq_enabled_branches", []))
    database = table_config.get("database") or dbt_config["database_mapping"][table_config["catalog"]]

    lines = [
        "version: 2",
        "",
        "sources:",
        f"  - name: {source_name}",
        '    description: "Silver-layer Iceberg tables produced by ml-feature-platform and consumed by ML feature pipelines."',
        f"    database: {database}",
        f"    schema: {source_schema}",
        "    meta:",
        f'      owner: "{dbt_config["owner"]}"',
        "    tables:",
        f"      - name: {table_config['name']}",
        "        description: >",
    ]
    lines.extend(_wrapped_description(str(table_config["description"]), 10))
    lines.extend(
        [
            f'        loaded_at_field: "{table_config["loaded_at_field"]}"',
            "        freshness:",
            "          error_after:",
            f"            count: {table_config['freshness']['error_after']['count']}",
            f"            period: {table_config['freshness']['error_after']['period']}",
        ]
    )

    if include_dq:
        lines.extend(_table_tests(table_config))

    lines.append("        columns:")
    for column in table_config["columns"]:
        lines.extend(_column_yaml(column, include_dq))

    return "\n".join(lines) + "\n"


def _source_schema(
    dbt_config: dict[str, Any],
    table_config: dict[str, Any],
    branch: str,
) -> str:
    return str(dbt_config.get("schema_overrides", {}).get(branch, table_config["schema"]))


def _table_tests(table_config: dict[str, Any]) -> list[str]:
    date_column = table_config["primary_key"][0]
    primary_key_not_null = " AND ".join(
        f"{column_name} IS NOT NULL" for column_name in table_config["primary_key"]
    )
    lines = [
        "        tests:",
        "          - dbt_utils.recency:",
        f"              field: {date_column}",
        "              datepart: day",
        "              interval: 2",
        "          - dbt_utils.expression_is_true:",
        f"              expression: \"{date_column} <= current_date\"",
        "          - dbt_utils.expression_is_true:",
        f"              expression: \"{date_column} >= DATE '2020-01-01'\"",
        "          - dbt_utils.expression_is_true:",
        f"              expression: \"{primary_key_not_null}\"",
        "          - dbt_utils.unique_combination_of_columns:",
        "              combination_of_columns:",
    ]
    lines.extend(f"                - {column_name}" for column_name in table_config["primary_key"])
    return lines


def _column_yaml(column: dict[str, Any], include_dq: bool) -> list[str]:
    lines = [
        f"          - name: {column['name']}",
        f'            description: "{column["description"]}"',
    ]
    if include_dq and column.get("tests"):
        lines.append("            tests:")
        for test in column["tests"]:
            lines.extend(_test_yaml(test, indent=14))
    return lines


def _test_yaml(test: Any, indent: int) -> list[str]:
    prefix = " " * indent
    if isinstance(test, str):
        return [f"{prefix}- {test}"]

    lines = []
    for test_name, test_config in test.items():
        lines.append(f"{prefix}- {test_name}:")
        for key, value in test_config.items():
            lines.append(f"{prefix}    {key}: {json.dumps(value, ensure_ascii=False)}")
    return lines


def _wrapped_description(description: str, indent: int) -> list[str]:
    prefix = " " * indent
    return [f"{prefix}{description}"]


def _yaml_files(models_path: Path) -> list[Path]:
    return sorted(list(models_path.glob("*.yml")) + list(models_path.glob("*.yaml")))


def _existing_tables(models_path: Path) -> set[str]:
    existing_tables = set()
    for yaml_path in _yaml_files(models_path):
        print(f"scan_yaml={yaml_path.relative_to(models_path)}")
        content = yaml_path.read_text(encoding="utf-8")
        existing_tables.update(TABLE_NAME_PATTERN.findall(content))
    return existing_tables


def _checkout_branch(runtime: RuntimeConfig, base_branch: str) -> str:
    branch_name = f"automation/ml-feature-platform-sources-{runtime.branch}"
    _run(["git", "checkout", base_branch], cwd=runtime.workspace)
    _run(["git", "checkout", "-B", branch_name], cwd=runtime.workspace)
    return branch_name


def _publish_changes(
    runtime: RuntimeConfig,
    branch_name: str,
    base_branch: str,
    created_files: list[Path],
) -> None:
    print("created_files=" + ", ".join(path.relative_to(runtime.workspace).as_posix() for path in created_files))
    _run(["git", "config", "user.email", "ci@ml-feature-platform.local"], cwd=runtime.workspace)
    _run(["git", "config", "user.name", "ml-feature-platform-ci"], cwd=runtime.workspace)
    _run(["git", "add", *[path.relative_to(runtime.workspace).as_posix() for path in created_files]], cwd=runtime.workspace)
    _run(["git", "status", "--short"], cwd=runtime.workspace)
    _run(["git", "commit", "-m", "Add ml-feature-platform dbt sources"], cwd=runtime.workspace)
    _run(["git", "push", "--force", "origin", branch_name], cwd=runtime.workspace)

    repo_slug = _github_slug(runtime.dbt_repo_url)
    if repo_slug:
        pr_result = _run(
            [
                "gh",
                "pr",
                "create",
                "--repo",
                repo_slug,
                "--base",
                base_branch,
                "--head",
                branch_name,
                "--title",
                "Add ml-feature-platform dbt sources",
                "--body",
                _pr_body(runtime),
            ],
            cwd=runtime.workspace,
            env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
            check=False,
            capture_output=True,
        )
        pr_url = (pr_result.stdout or "").strip()
        if pr_result.returncode == 0 and pr_url:
            print(f"created_pr_url={pr_url}")
            _write_created_pr_url(runtime, pr_url)
            _comment_source_pr_if_possible(runtime, pr_url)
        else:
            print(f"gh pr create finished with code={pr_result.returncode}")
            if pr_result.stdout:
                print(pr_result.stdout.strip())
            if pr_result.stderr:
                print(pr_result.stderr.strip())
    else:
        print("Cannot derive GitHub repo slug from DBT_REPO_URL, skip PR creation")


def _pr_body(runtime: RuntimeConfig) -> str:
    lines = ["Generated by ml-feature-platform CI."]
    if runtime.build_url:
        lines.append("")
        lines.append(f"Drone build: {runtime.build_url}")
    return "\n".join(lines)


def _write_created_pr_url(runtime: RuntimeConfig, pr_url: str) -> None:
    output_path = Path.cwd() / "created_dbt_pr_url.txt"
    output_path.write_text(pr_url + "\n", encoding="utf-8")
    print(f"created_pr_url_file={output_path}")


def _comment_source_pr_if_possible(runtime: RuntimeConfig, pr_url: str) -> None:
    source_pr = os.getenv("DRONE_PULL_REQUEST")
    source_repo = os.getenv("DRONE_REPO")
    if not source_pr or not source_repo:
        print("No source PR context in Drone, skip source PR comment")
        return

    print(f"Comment source PR #{source_pr} in {source_repo}")
    _run(
        [
            "gh",
            "pr",
            "comment",
            source_pr,
            "--repo",
            source_repo,
            "--body",
            f"Created dbt source PR: {pr_url}",
        ],
        env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
        check=False,
    )


def _with_token(repo_url: str, token: str) -> str:
    repo_url = _normalize_repo_url(repo_url)
    if repo_url.startswith("https://"):
        return repo_url.replace("https://", f"https://{token}@", 1)
    return repo_url


def _normalize_repo_url(repo_url: str) -> str:
    repo_url = repo_url.strip()
    if repo_url.startswith("github.com/"):
        repo_url = f"https://{repo_url}"
    if repo_url.startswith("https://github.com/") and not repo_url.endswith(".git"):
        repo_url = f"{repo_url}.git"
    return repo_url


def _github_slug(repo_url: str) -> Optional[str]:
    repo_url = _normalize_repo_url(repo_url)
    match = re.search(r"github\.com[:/](?P<slug>[^/]+/[^/.]+)(?:\.git)?$", repo_url)
    if not match:
        return None
    return match.group("slug")


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


def _run(
    command: list[str],
    cwd: Optional[Path] = None,
    env: Optional[dict[str, str]] = None,
    check: bool = True,
    capture_output: bool = False,
) -> subprocess.CompletedProcess:
    printable = " ".join(_mask_token(part) for part in command)
    cwd_text = f" cwd={cwd}" if cwd else ""
    print(f"+{cwd_text} {printable}")
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.stdout:
        print(_mask_token(result.stdout.rstrip()))
    if result.stderr:
        print(_mask_token(result.stderr.rstrip()))
    if check and result.returncode != 0:
        print(f"command_failed_returncode={result.returncode}")
        print_clone_failure_hint(command)
        raise subprocess.CalledProcessError(
            result.returncode,
            [_mask_token(part) for part in command],
            output=_mask_token(result.stdout or ""),
            stderr=_mask_token(result.stderr or ""),
        )
    return result


def print_clone_failure_hint(command: list[str]) -> None:
    if len(command) >= 2 and command[0] == "git" and command[1] in {"clone", "ls-remote"}:
        print("git_auth_hint=Check DBT_REPO_URL, GIT_TOKEN repo access, org SSO approval, and contents read permission.")


def _mask_token(value: str) -> str:
    value = re.sub(r"x-access-token:[^@]+@", "x-access-token:***@", value)
    value = re.sub(r"https://github_pat_[^@]+@", "https://github_pat_***@", value)
    value = re.sub(r"https://ghp_[^@]+@", "https://ghp_***@", value)
    return value


def _log_section(title: str) -> None:
    print("")
    print(f"=== {title} ===")


if __name__ == "__main__":
    raise SystemExit(main())
