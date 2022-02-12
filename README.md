# Lab Big Data
This lab environment is composed by:

- Apache Spark

- Apache Cassandra

- Apache Airflow

- Jupiter Lab

The initial idea is to do a ingestion from a a datasource (flat files) to Cassandra using Airflow and Spark. 

![alt text](docs/images/banestes_datalake.jpg)

## Todo

- [ ] Minio Integration with others services
- [ ] Alyra correct integration - Save Dags in Apache Airflow

## Set-up 

### Clone repository

```bash
git clone {repository}
```

### Install Docker and Docker-compose

Follow [documentation](https://docs.docker.com/engine/install/) for Docker installation and [documentation](https://docs.docker.com/compose/install/) for Docker-compose installation


## For developing and debugging

### Install Visual Code

Follow [documentation](https://code.visualstudio.com/download)

#### Extensions

Install an extension so that you can develop using the containers created by docker compose. Access Extensions visualization (Ctrl+Shift+x) and search for extension "Remote - Containers" aka id "ms-vscode-remote.remote-containers"

![alt text](docs/images/vscode_extension.png)

#### Notebooks

Visual Code makes it possible to run notebooks remotely using an existing Jupyter service. 

Once a notebook(.ipyn) file is opened inside Visual Code, if it doesn't automatically open the docker-compose service. In the lower right corner, there will be the option to select the Remote service

![alt text](docs/images/visualcode_jupyterremoto.png)

You will be prompted for the URI of the Jupyterlab service. Example: http://172.20.013:8888

![alt text](docs/images/visualcode_jupyterremotoprompt.png)

And then you will be asked for the service password. The password is datalake

![alt text](docs/images/visualcode_jupyterremotopromptpassword.png)

## For execute in standalone mode

### Execute Docker-compose 

```bash
docker-compose up -d --remove-orphans --force-recreate
```

### Access services

#### - Jupyterlab

http://172.20.0.13:8888?token=datalake

#### - Apache Airflow

http://172.20.0.4:8080/home

user: airflow

password: airflow

#### - Apache Spark

http://172.20.0.9:8080/

#### - Cassandra

Main IP: 172.20.0.11

```bash
docker exec -it datalake_cassandra  cqlsh  -u cassandra -p cassandra 
```

via web interface

http://172.20.0.12:3000/


or using Cassandra Extension in Visual Code

![alt text](docs/images/visualcode_cassandraworkbanch.png)


### Folders


```
|-- .cassandraWorkbench                            CassandraWorkbench extension folder
|-- .devcontainer                                  Set of development container definitions
|   |-- devcontainer.json                          Container configuration file
|   |-- docker-compose.yml                         Extra settings for running docker-compose file
|-- .vscode                                        Contain settings, task settings, and debug settings
|   |-- settings.json                              Extension settings for linters and code formatting tools to enforce the language rules used in this repo
|-- dags                                           Folder where the Apache Airflow Dags configuration files will be stored.
|   |-- spark-cassandra-dag.py                     Example Dag file that runs an app(python) that will read from a text file and insert into cassandra tables
|   |-- spark-cassandra-dag_withPapermill.py       Example Dag file that runs Notebook using Papermill extension, which in turn will read from a text file and insert into cassandra tables
|   |-- spark-dag.py                               Dag file example that runs an application (python) that only checks if the connection to spark is working
|-- data                                           Shared folder between Apache Airflow, JupiterLab and Apache Spark that stores raw files
|-- docs                                           Folder for other types of documentation
|-- logs                                           Log storage of all services
|-- notebooks                                      Shared folder between Apache Airflow, JupiterLab and Apache Spark that stores Pyspark Notebooks
|   |-- spark-book-count.ipynb                     Notebook that only checks if the connection to spark is working
|   |-- spark-extract-and-load.ipynb               Notebook that will read from a text file and insert into cassandra tables
|-- spark-apps                                     Shared folder between Apache Airflow, JupiterLab and Apache Spark that stores Python applications
|   |-- spark-book-count.py                        Python application that only checks if the connection to spark is working
|   |-- spark-cassandra-test.py                    Python application that will read from a text file and insert into cassandra tables
|-- docker-compose.yaml                            Docker-compose file
|-- Dockerfile                                     Apache Airflow docker file. Required for Java to be installed on the service
|-- README.md                                      Readme file
|-- .cassandraWorkbench.jsonc                      CassandraWorkbanch extension configuration. In this file is Cassandra's host address
```

# Examples

In this project, there are some examples made in both Python and Notebooks. In some cases it is necessary to create objects within Cassandra. Follow the [documentation](#create-keyspace-and-tables-in-cassandra).

## Dependency 

### Create keyspace and tables in cassandra

Execute script in Cassandra database using the various methods described in the [documentation](#cassandra).


>create keyspace test with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

>CREATE TABLE test.previous_employees_by_job_title (
    job_title text,
    employee_id uuid,
    employee_name text,
    first_day timestamp,
    last_day timestamp,
    PRIMARY KEY (job_title, employee_id)
) WITH CLUSTERING ORDER BY (employee_id ASC);

>CREATE TABLE test.days_worked_by_previous_employees_by_job_title (
    job_title text,
    employee_id uuid,
    employee_name text,
    days_worked int,
    PRIMARY KEY (job_title, employee_id)
) WITH CLUSTERING ORDER BY (employee_id ASC);

# Production enviroment

For the application of this environment in production, in addition to using multiple hosts to accommodate all services, some extra configurations are needed to ensure integration between services.

For other details not documented here, check the settings that are in docker-compose.

## Storage

There must be a single volume on the network that can be accessed by the Apache Spark, Jupyter Hub, and Apache Airflow services. This is necessary so that all services can see and maintain Apache Airflow Dags, Applications or Notebooks that will be run by Apache Spark and accessed by Apache Airflow. It is recommended to use a scalable, high-performance storage layer.

## Versions

So that all services can communicate correctly, it is mandatory that Spark core versions and Java installation are the same.

## Integration between Apache Airflow and Apache Spark

In order for Apache Airflow to have a standard connection with Apache Spark, the following command must be executed inside the host where Apache Airflow is installed

> airflow connections add "spark_default" --conn-host "spark://{ip host spark}:{port host spark}" --conn-type "spark"

However, this configuration can also be done using the Apache Spark web interface.

![alt text](docs/images/ApacheAirflow_connection.png)

## Using Notebooks 

### Python Kernel on Apache Airflow

Installing a Python kernel so notebooks can be referenced by Apache Airflow within Dags.

> pip install ipython ipykernel

> ipython kernel install --name "python3"

## Install Papermill

Apache Airflow supports integration with Papermill. Papermill is a tool for parameterizing and executing Jupyter Notebooks. For more information check the [documentation](https://airflow.apache.org/docs/apache-airflow-providers-papermill/stable/operators.html).

> pip install apache-airflow-providers-papermill==2.2.0

