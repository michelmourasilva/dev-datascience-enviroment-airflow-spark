from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.papermill_operator import PapermillOperator

import os
import sys

args = {
    'owner': 'Airflow',
}

with DAG(
    dag_id='spark-test-cassandra-papermill',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['papermill'],
) as dag:

    load_and_write = PapermillOperator(
        task_id="load_and_write_job_papermill",
        input_nb="/usr/local/spark/notebooks/spark-extract-and-load.ipynb",
        output_nb="/tmp/out-{{ execution_date }}.ipynb",
#        kernel_name="ipykernel",
#        parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"},
    )


    load_and_write
