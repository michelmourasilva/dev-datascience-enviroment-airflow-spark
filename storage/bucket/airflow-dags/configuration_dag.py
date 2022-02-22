from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


with DAG(dag_id='configuration_dag', 
		 schedule_interval="@once", 
		 start_date=datetime(2020, 1, 1), 
		 catchup=False) as dag:

	# Task 2
	bash_task = BashOperator(task_id='bash_task', 
							 bash_command="/usr/local/spark/configure.sh ")

		
	bash_task 
