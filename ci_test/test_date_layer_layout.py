"""Date-only calendars have an explicit group, not an artificial entity key."""

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_script import validate_layer_layout


@pytest.mark.parametrize("group,key,column,expected", [
    ("date", "date", "date DATE", None),
    ("calendar_id", "date", "date DATE", "must be 'date'"),
    ("date", "captured_at", "captured_at TIMESTAMP", "no non-date columns"),
    ("date", "date,calendar_id", "date DATE", "must be 'calendar_id'"),
])
def test_date_only_group(tmp_path, group, key, column, expected):
    entity = tmp_path / "layers" / "silver" / group / "calendar" / "v1"
    (entity / "migrations").mkdir(parents=True)
    dag_id = f"feature-platform.layers.silver.{group}.calendar"
    (entity / "config.yaml").write_text(
        f"table:\n  primary_key: {key}\ndag:\n  id: {dag_id}\n"
    )
    (entity / "migrations/create_table.sql").write_text(f"CREATE TABLE sample (\n {column},\n x INT\n)")
    (entity / "README.md").write_text(dag_id)
    (entity.parents[1] / "README.md").write_text("calendar/v1/README.md")
    (entity.parents[2] / "README.md").write_text(f"{group}/README.md")
    errors = validate_layer_layout(tmp_path)
    if expected is None:
        assert errors == []
    else:
        assert any(expected in error for error in errors), errors
