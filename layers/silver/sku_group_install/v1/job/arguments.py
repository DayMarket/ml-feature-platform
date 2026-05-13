from argparse import ArgumentParser

from job.entities import Arguments

def parse_arguments() -> Arguments:
    parser = ArgumentParser()
    parser.add_argument("--trigger_date", type=str, required=True)
    parser.add_argument("--table_name", type=str, required=True)

    namespace, _ = parser.parse_known_args()

    return Arguments(
        trigger_date=namespace.trigger_date,
        table_name=namespace.table_name,
    )
