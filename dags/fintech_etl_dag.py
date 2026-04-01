from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Añadimos la raíz del proyecto al path para que Airflow pueda importar main.py y scripts/
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/..'))

# Importamos la función
from main import run_pipeline_once

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1), # Fecha en el pasado para que empiece de inmediato
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'fintech_etl_pipeline',
    default_args=default_args,
    description='Pipeline de Ingesta y Detección de Fraude',
    schedule_interval=timedelta(minutes=1), # Reemplaza time.sleep(60)
    catchup=False, # Evita que ejecute todo el historial
    tags=['fintech', 'etl', 'fraud_detection'],
) as dag:

    # Definimos la tarea única que llama a nuestro código
    run_etl_task = PythonOperator(
        task_id='extract_transform_load',
        python_callable=run_pipeline_once,
    )

    run_etl_task