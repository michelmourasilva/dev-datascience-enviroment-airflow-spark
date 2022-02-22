from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.papermill_operator import PapermillOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

import os
import sys

#create a datetime variable
now = datetime.now()

#Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id='spark-test-cassandra-papermill',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['papermill'],
) as dag:


	configuration = TriggerDagRunOperator(
		task_id="trigger_config_dag",
		trigger_dag_id="configuration_dag",
		execution_date='{{ ts }}',
		reset_dag_run=True,
    		wait_for_completion=True,
		poke_interval=30
	)	
	
    # Create objects in cassandra if not exists    
	create_objects=TriggerDagRunOperator(
		task_id="trigger_create_objects",
		trigger_dag_id="cassandra_create_test_database_dag",
		execution_date='{{ ts }}',
		reset_dag_run=True,
    		wait_for_completion=True,
		poke_interval=60
	)	

    # Execute notebooks
	load_and_write = PapermillOperator(
        task_id="load_and_write_job_papermill",
        input_nb="/usr/local/spark/notebooks/spark-extract-and-load.ipynb",
        output_nb="/tmp/out-{{ execution_date }}.ipynb",
#        kernel_name="ipykernel",
        parameters={"origin": "Airflow"},
    )


	configuration >> create_objects >> load_and_write
