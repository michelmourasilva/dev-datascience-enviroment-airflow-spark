#!/bin/bash
airflow connections add "spark_default" --conn-host "spark://172.20.0.9:7077" --conn-type "spark"     
airflow connections add "minio_default" --conn-login "minio" --conn-password "minio123" --conn-type "s3" --conn-extra "{\'host\': \'http://172.20.0.14:9000\'}" 
airflow connections add "cassandra_default" --conn-login "cassandra" --conn-password "cassandra" --conn-type "cassandra" --conn-host "172.20.0.11" --conn-port "9042"
ipython kernel install --user --name "python3"

