from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4),
}

# Шлях до DAG-файлів усередині контейнера
dags_dir = "/opt/airflow/dags"

with DAG(
    'final_project_BBDL',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["OLena"],
    description="ETL pipeline: Landing → Bronze → Silver → Gold"
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id='LandingToBronze',
        application=os.path.join(dags_dir, 'landing_to_bronze.py'),
        name='LandingToBronze',
        conn_id="spark_default",
        conf={
        "spark.master": "spark://spark-master:7077"        
    },
        verbose=True
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='BronzeToSilver',
        application=os.path.join(dags_dir, 'bronze_to_silver.py'),
        name='BronzeToSilver',
        conn_id="spark_default",
        conf={
        "spark.master": "spark://spark-master:7077",
        "spark.submit.deployMode": "client",
        "spark.pyspark.python": "/usr/bin/python3",
        "spark.executorEnv.PYSPARK_PYTHON": "/usr/bin/python3"
    },
        verbose=True
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='SilverToGold',
        application=os.path.join(dags_dir, 'silver_to_gold.py'),
        name='SilverToGold',
        conn_id="spark_default",
        conf={
        "spark.master": "spark://spark-master:7077",
        "spark.submit.deployMode": "client",
        "spark.pyspark.python": "/usr/bin/python3",
        "spark.executorEnv.PYSPARK_PYTHON": "/usr/bin/python3"
    },
        verbose=True
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
