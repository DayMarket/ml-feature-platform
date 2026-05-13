import os
import sys
import logging
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from airflow_commons.helpers.oncall import send_oncall_notification
DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)
from сonfig.factory import get_deployment

logger = logging.getLogger("airflow.task")
logger.setLevel('INFO')

default_args = {
    'owner': 'team:search',
    'depends_on_past': False,
    'trigger_rule': 'all_success',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_oncall_notification(
        severity='P3', 
        team='search',
        oncall_webhook_conn_id="oncall_webhook_search"
    ),
}
@dag(
    default_args=default_args,
    max_active_runs=1,
    tags=['spark', 'feature-platform', 'team::search', 'silver'],
    is_paused_upon_creation=True,
    schedule_interval='0 1 * * *',
    start_date=datetime(2026, 1, 1, 0, 0, 0),
    dag_id='feature_platform_sku_group_install_silver_stats_dag'
)
def collect_silver_sku_group_query_install_stats():

    task_id = 'getting_sku_group_query_install_stats'
    getting_sku_group_query_features_from_events = SparkKubernetesOperator(
        execution_timeout=timedelta(minutes=3*60),
        task_id=task_id,
        namespace='svc-data-spark-jobs',
        application_file=get_deployment(
            '../config',
            'get_search_sku_group_silver_stats.yaml',
        ),
        kubernetes_conn_id='spark_k8s',
    )

    getting_sku_group_query_features_from_events


dag = collect_silver_sku_group_query_install_stats()