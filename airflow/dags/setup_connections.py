from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.utils.session import provide_session
import os
import sqlalchemy
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_connections(**context):
    """Create necessary connections in Airflow DB."""
    
    @provide_session
    def create_conn(session=None):
        # PostgreSQL connection
        pg_conn = Connection(
            conn_id='postgres_metadata_conn',
            conn_type='postgres',
            host=os.environ.get('POSTGRES_METADATA_HOST'),
            schema=os.environ.get('POSTGRES_METADATA_DB'),
            login=os.environ.get('POSTGRES_METADATA_USER'),
            password=os.environ.get('POSTGRES_METADATA_PASSWORD'),
            port=int(os.environ.get('POSTGRES_METADATA_PORT', 5432))
        )
        
        # ClickHouse connection - use HTTP type for compatibility with clickhouse-connect
        ch_host = os.environ.get('CLICKHOUSE_HOST')
        ch_port = int(os.environ.get('CLICKHOUSE_PORT_HTTP', 8123))  # Using HTTP port 8123 instead of native port 9000
        ch_user = os.environ.get('CLICKHOUSE_USER')
        ch_password = os.environ.get('CLICKHOUSE_PASSWORD')
        
        # Create extra JSON with detailed connection parameters
        extra_dict = {
            "verify": False,
            "database": "default",
            "connect_timeout": 10,
            "send_receive_timeout": 300,
            "client_name": "airflow_clickhouse"
        }
        
        ch_conn = Connection(
            conn_id='clickhouse_external_conn',
            conn_type='http',  # Use http as the conn_type for clickhouse-connect
            host=ch_host,
            port=ch_port,
            login=ch_user,
            password=ch_password,
            extra=json.dumps(extra_dict)
        )
        
        # Redis connection
        redis_conn = Connection(
            conn_id='redis_local_conn',
            conn_type='redis',
            host=os.environ.get('REDIS_HOST', 'redis'),
            port=int(os.environ.get('REDIS_PORT', 6379))
        )
        
        # Kafka connection
        kafka_conn = Connection(
            conn_id='kafka_local_conn',
            conn_type='kafka',
            host=os.environ.get('KAFKA_BROKERS_HOST', 'kafka'),
            port=int(os.environ.get('KAFKA_BROKERS_PORT', 9093)),
            extra='{"topics": ["' + os.environ.get('KAFKA_TOPIC_RAW', 'shared_raw_events') + 
                  '", "' + os.environ.get('KAFKA_TOPIC_DLQ', 'shared_dead_letter') + '"]}'
        )
        
        # For each connection, either create it or update it if it exists
        for conn in [pg_conn, ch_conn, redis_conn, kafka_conn]:
            try:
                session.query(Connection).filter(Connection.conn_id == conn.conn_id).delete()
                session.add(conn)
                print(f"Connection {conn.conn_id} created/updated successfully")
            except sqlalchemy.exc.IntegrityError:
                session.rollback()
                print(f"Connection {conn.conn_id} already exists, updating")
                session.merge(conn)
            except Exception as e:
                print(f"Error creating/updating connection {conn.conn_id}: {e}")
                session.rollback()
                raise
    
    create_conn()
    return "Connections created successfully"

with DAG(
    'setup_connections',
    default_args=default_args,
    description='Set up necessary connections in Airflow',
    schedule_interval=None,  # Run manually or on fixed schedule to ensure connections exist
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['setup', 'connections'],
) as dag:
    
    create_connections_task = PythonOperator(
        task_id='create_connections',
        python_callable=create_connections,
        provide_context=True
    ) 