from dataclasses import dataclass


@dataclass
class Arguments:
    run_date: str
    config_path: str
    repo_root: str
    feature_groups: str | None
    kafka_topic: str
    kafka_brokers: str
    kafka_login: str
    kafka_password: str
