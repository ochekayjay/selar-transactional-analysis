"""
### DAG which produces to and consumes from a Kafka cluster
This DAG will produce messages consisting of several elements to a Kafka cluster and consume
them.
"""
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dag_parsing_context import get_parsing_context
from confluent_kafka import Consumer, KafkaException, KafkaError
from airflow.utils.log.logging_mixin import LoggingMixin
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig,RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import ExecutionMode
import pandas as pd


@dag(
    start_date=datetime(2025, 9, 23),
    schedule=None,   # run on demand
    catchup=False,
)
def test_consumption():

    @task
    def print_random():
        print("I am here")

    printedVal = print_random()\

test_consumption()

