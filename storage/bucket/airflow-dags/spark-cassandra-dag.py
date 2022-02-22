from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.cassandra.sensors.record import CassandraRecordSensor
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
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


def check_table_exists(keyspace_name, table_name):
	hook = CassandraHook('cassandra_default')
	
	print('Cheking for existence of {}.{}'.format(keyspace_name, table_name))
	hook.keyspace = keyspace_name
	return hook.table_exists(table_name)	


with DAG(
    dag_id='spark-test-cassandra',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['cassandra', 'spark'],	
) as dag:
    
    # Create connections and dependences if not exists
	configuration = TriggerDagRunOperator(
		task_id="trigger_config_dag",
		trigger_dag_id="configuration_dag",
		execution_date='{{ ds }}',
		reset_dag_run=True,
    	wait_for_completion=True,
		poke_interval=30
	)	
	
	# Create objects in cassandra if not exists
	create_objects=TriggerDagRunOperator(
		task_id="trigger_create_objects",
		trigger_dag_id="cassandra_create_test_database_dag",
		execution_date='{{ ds }}',
		reset_dag_run=True,
    	wait_for_completion=True,
		poke_interval=30
	)	
	
	# check if first table exists
	table_check_1 = PythonOperator(
		task_id = 'table_check_1',
		python_callable=check_table_exists,
		op_args = ['test','previous_employees_by_job_title']
	)

	# check if second table exists
	table_check_2 = PythonOperator(
		task_id = 'table_check_2',
		python_callable=check_table_exists,
		op_args = ['test','days_worked_by_previous_employees_by_job_title']
	)		  
		   		  
		  
	load_and_write = BashOperator(
        task_id="load_and_write_job",
        bash_command='spark-submit \
            --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
            /usr/local/spark/app/spark-cassandra-test.py'
    )

	configuration >> create_objects >> table_check_1 >> table_check_2 >> load_and_write
