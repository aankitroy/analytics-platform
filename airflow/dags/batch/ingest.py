from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_batch_data(**context):
    """
    Ingest batch data for a client.
    This includes:
    1. Reading batch data from source
    2. Validating and transforming data
    3. Loading into ClickHouse
    4. Updating metadata in PostgreSQL
    """
    # Get ingestion details from the DAG run configuration
    client_id = context['dag_run'].conf.get('client_id')
    data_source = context['dag_run'].conf.get('data_source')
    start_date = context['dag_run'].conf.get('start_date')
    end_date = context['dag_run'].conf.get('end_date')
    
    if not all([client_id, data_source, start_date, end_date]):
        raise ValueError("client_id, data_source, start_date, and end_date are required in the DAG run configuration")

    # TODO: Implement batch ingestion logic
    # 1. Read batch data from source
    # 2. Validate and transform data
    # 3. Load into ClickHouse
    # 4. Update metadata in PostgreSQL

with DAG(
    'batch_ingest',
    default_args=default_args,
    description='Ingest batch data for clients',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['batch', 'ingestion'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_batch_data',
        python_callable=ingest_batch_data,
        provide_context=True,
    ) 