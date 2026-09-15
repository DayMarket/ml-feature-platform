from dataclasses import dataclass


@dataclass
class Arguments:
    partition_start: str
    partition_end: str
    table_name: str
