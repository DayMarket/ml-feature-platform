from dataclasses import dataclass


@dataclass(frozen=True)
class Arguments:
    partition_start: str
    partition_end: str
    table_name: str


@dataclass(frozen=True)
class DatasetSettings:
    model_name: str
    sample_percent: int
