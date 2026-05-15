from argparse import ArgumentParser

from job.entities import Arguments

def parse_arguments() -> Arguments:
    parser = ArgumentParser()
    parser.add_argument("--partition_start", type=str, required=True)
    parser.add_argument("--partition_end", type=str, required=True)
    parser.add_argument("--table_name", type=str, required=True)

    namespace, _ = parser.parse_known_args()

    return Arguments(
        partition_start=namespace.partition_start,
        partition_end=namespace.partition_end,
        table_name=namespace.table_name,
    )
