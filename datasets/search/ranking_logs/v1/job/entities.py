from dataclasses import dataclass


@dataclass(frozen=True)
class Arguments:
    partition_start: str
    partition_end: str
    table_name: str


@dataclass(frozen=True)
class DatasetSettings:
    model_name: str
    # Процент запросов, а не доля: 7 — это 7%, 0.01 — одна сотая процента.
    # float, а не int: на разведочных ранах доля опускается ниже процента.
    sample_percent: float
