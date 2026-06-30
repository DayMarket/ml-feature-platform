from dataclasses import dataclass


@dataclass(frozen=True)
class Arguments:
    partition_start: str
    partition_end: str
    table_name: str
