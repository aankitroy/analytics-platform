from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import random
import json

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Python functions
def extract_data(**kwargs):
    """Simulate extracting data from a source"""
    # Simulate some data
    data = {
        'timestamp': datetime.now().isoformat(),
        'value': random.randint(1, 100),
        'status': 'success'
    }
    
    # Push the data to XCom
    kwargs['ti'].xcom_push(key='extracted_data', value=json.dumps(data))
    return data

def transform_data(**kwargs):
    """Transform the extracted data"""
    # Pull the data from XCom
    data = json.loads(kwargs['ti'].xcom_pull(task_ids='extract_data', key='extracted_data'))
    
    # Transform the data
    transformed_data = {
        'timestamp': data['timestamp'],
        'value_squared': data['value'] ** 2,
        'status': data['status'],
        'processed_at': datetime.now().isoformat()
    }
    
    # Push the transformed data to XCom
    kwargs['ti'].xcom_push(key='transformed_data', value=json.dumps(transformed_data))
    return transformed_data

def load_data(**kwargs):
    """Simulate loading data to a destination"""
    # Pull the transformed data from XCom
    data = json.loads(kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data'))
    
    # Simulate loading data
    print(f"Loading data: {data}")
    return data

# Create the DAG
with DAG(
    'example_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline example',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example'],
    catchup=False
) as dag:

    # Define tasks
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task

    # Add a parallel task that runs after extract
    parallel_task = BashOperator(
        task_id='parallel_task',
        bash_command='echo "Running parallel task at $(date)"',
    )

    # Add the parallel task to the workflow
    extract_task >> parallel_task 