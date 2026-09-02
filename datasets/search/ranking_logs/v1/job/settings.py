import os
from pathlib import Path
from typing import Dict, Union

from job.entities import DatasetSettings

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _read_block(config_path: Union[str, os.PathLike], block: str) -> Dict[str, str]:
    """Возвращает скалярные ключи одного верхнеуровневого блока config.yaml."""
    values: Dict[str, str] = {}
    inside = False
    with open(config_path, "r", encoding="utf-8") as config_file:
        for raw_line in config_file:
            if not raw_line.strip() or raw_line.lstrip().startswith("#"):
                continue
            indent = len(raw_line) - len(raw_line.lstrip(" "))
            line = raw_line.strip()
            if indent == 0:
                inside = line.rstrip(":") == block
                continue
            if not inside:
                continue
            key, separator, value = line.partition(":")
            if separator and value.strip():
                values[key.strip()] = _unquote(value.strip())
    return values


def load_dataset_settings(
    config_path: Union[str, os.PathLike] = DEFAULT_CONFIG_PATH,
) -> DatasetSettings:
    block = _read_block(config_path, "dataset")

    model_name = block.get("model_name", "")
    if not model_name:
        raise ValueError(f"dataset.model_name is missing in {config_path}")

    # float, а не int: значение задаётся в процентах и бывает дробным
    # (0.01 — одна сотая процента). int("0.01") падал ValueError и валил джоб
    # на загрузке настроек, до старта Spark. Нижнюю границу шага задаёт сетка
    # бакетов — её сторожит query.sample_threshold, единственный владелец
    # HASH_BUCKETS.
    sample_percent = float(block.get("sample_percent", 0))
    if not 0 < sample_percent <= 100:
        raise ValueError(
            f"dataset.sample_percent must be in (0, 100], got {sample_percent}"
        )

    return DatasetSettings(model_name=model_name, sample_percent=sample_percent)
