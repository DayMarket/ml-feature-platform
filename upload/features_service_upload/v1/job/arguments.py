from argparse import ArgumentParser

from job.entities import Arguments


def parse_arguments() -> Arguments:
    parser = ArgumentParser()
    parser.add_argument("--run_date", type=str, required=True)
    parser.add_argument("--config_path", type=str, required=True)
    parser.add_argument("--repo_root", type=str, required=True)
    parser.add_argument("--feature_groups", type=str, required=False)
    parser.add_argument("--kafka_topic", type=str, required=True)
    parser.add_argument("--kafka_brokers", type=str, required=True)
    parser.add_argument("--kafka_login", type=str, required=True)
    parser.add_argument("--kafka_password", type=str, required=True)

    namespace, _ = parser.parse_known_args()
    return Arguments(
        run_date=namespace.run_date,
        config_path=namespace.config_path,
        repo_root=namespace.repo_root,
        feature_groups=namespace.feature_groups,
        kafka_topic=namespace.kafka_topic,
        kafka_brokers=namespace.kafka_brokers,
        kafka_login=namespace.kafka_login,
        kafka_password=namespace.kafka_password,
    )
