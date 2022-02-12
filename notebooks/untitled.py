from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import DAG
from airflow.utils.dates import days_ago


args = {
    "project_id": "untitled-0212164427",
}

dag = DAG(
    "untitled-0212164427",
    default_args=args,
    schedule_interval="@once",
    start_date=days_ago(1),
    description="Created with Elyra 3.6.0 pipeline editor using `untitled.pipeline`.",
    is_paused_upon_creation=False,
)


# Operator source: work/spark-book-count.ipynb
op_43f3581b_c5f9_45ae_ae51_7efd3157903e = KubernetesPodOperator(
    name="spark_book_count",
    namespace="airflow",
    image="continuumio/anaconda3:2020.07",
    cmds=["sh", "-c"],
    arguments=[
        "mkdir -p ./jupyter-work-dir/ && cd ./jupyter-work-dir/ && curl -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v3.6.0/elyra/airflow/bootstrapper.py --output bootstrapper.py && curl -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v3.6.0/etc/generic/requirements-elyra.txt --output requirements-elyra.txt && python3 -m pip install packaging && python3 -m pip freeze > requirements-current.txt && python3 bootstrapper.py --cos-endpoint http://172.20.0.14:9000 --cos-bucket teste --cos-directory 'untitled-0212164427' --cos-dependencies-archive 'spark-book-count-43f3581b-c5f9-45ae-ae51-7efd3157903e.tar.gz' --file 'work/spark-book-count.ipynb' "
    ],
    task_id="spark_book_count",
    env_vars={
        "ELYRA_RUNTIME_ENV": "airflow",
        "AWS_ACCESS_KEY_ID": "minio",
        "AWS_SECRET_ACCESS_KEY": "minio123",
        "ELYRA_ENABLE_PIPELINE_INFO": "True",
        "ELYRA_RUN_NAME": "untitled-0212164427-{{ ts_nodash }}",
    },
    in_cluster=True,
    config_file="None",
    dag=dag,
)


# Operator source: work/spark-extract-and-load.ipynb
op_b75b13a5_75e1_48bc_a0db_8951a664eb81 = KubernetesPodOperator(
    name="spark_extract_and_load",
    namespace="airflow",
    image="continuumio/anaconda3:2020.07",
    cmds=["sh", "-c"],
    arguments=[
        "mkdir -p ./jupyter-work-dir/ && cd ./jupyter-work-dir/ && curl -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v3.6.0/elyra/airflow/bootstrapper.py --output bootstrapper.py && curl -H 'Cache-Control: no-cache' -L https://raw.githubusercontent.com/elyra-ai/elyra/v3.6.0/etc/generic/requirements-elyra.txt --output requirements-elyra.txt && python3 -m pip install packaging && python3 -m pip freeze > requirements-current.txt && python3 bootstrapper.py --cos-endpoint http://172.20.0.14:9000 --cos-bucket teste --cos-directory 'untitled-0212164427' --cos-dependencies-archive 'spark-extract-and-load-b75b13a5-75e1-48bc-a0db-8951a664eb81.tar.gz' --file 'work/spark-extract-and-load.ipynb' "
    ],
    task_id="spark_extract_and_load",
    env_vars={
        "ELYRA_RUNTIME_ENV": "airflow",
        "AWS_ACCESS_KEY_ID": "minio",
        "AWS_SECRET_ACCESS_KEY": "minio123",
        "ELYRA_ENABLE_PIPELINE_INFO": "True",
        "ELYRA_RUN_NAME": "untitled-0212164427-{{ ts_nodash }}",
    },
    in_cluster=True,
    config_file="None",
    dag=dag,
)

op_b75b13a5_75e1_48bc_a0db_8951a664eb81 << op_43f3581b_c5f9_45ae_ae51_7efd3157903e
