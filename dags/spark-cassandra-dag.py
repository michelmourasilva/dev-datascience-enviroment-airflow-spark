from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

import os
import sys

args = {
    'owner': 'Airflow',
}

with DAG(
    dag_id='spark-test-cassandra',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    # [START howto_operator_spark_submit]
    #os.environ['SPARK_HOME'] = '/workspace/example-cassandra-etl-with-airflow-and-spark/spark-3.0.1-bin-hadoop2.7'
    #sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

    load_and_write = BashOperator(
        task_id="load_and_write_job",
        bash_command='spark-submit \
            --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
            /usr/local/spark/app/spark-cassandra-test.py'
    )


    load_and_write
    # [END howto_operator_spark_submit]


