from airflow.models import DAG
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging

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


# Create objects for sample tests
def migration_cassandra():

	try:
	
		hook = CassandraHook('cassandra_default')	
		session = hook.get_conn()

		cqls = [
			"CREATE KEYSPACE IF NOT EXISTS test with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",		
			"CREATE TABLE IF NOT EXISTS test.previous_employees_by_job_title ( job_title text, employee_id uuid, employee_name text, first_day timestamp, last_day timestamp, PRIMARY KEY (job_title, employee_id) ) WITH CLUSTERING ORDER BY (employee_id ASC)",
			"CREATE TABLE IF NOT EXISTS test.days_worked_by_previous_employees_by_job_title ( job_title text, employee_id uuid, employee_name text, days_worked int, PRIMARY KEY (job_title, employee_id) ) WITH CLUSTERING ORDER BY (employee_id ASC)",
		]		  
		for cql in cqls:
			session.execute(cql)

		session.shutdown()
		hook.shutdown_cluster()

		logging.info('>>>>>>>>>>> Executed: Keyspace and Tables are created')
		
	except Exception as e:

		logging.error('>>>>>>>>>>> Error: {}'.format(e))
		

with DAG(
    dag_id='cassandra_create_test_database_dag',
    default_args=default_args,
    schedule_interval="@once",    
    tags=['cassandra', 'create_objetcts'],
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
	
    # Execute create objects in cassandra
    create_table = PythonOperator(
        task_id = 'create_table',
        python_callable=migration_cassandra
    )    

    configuration >> create_table