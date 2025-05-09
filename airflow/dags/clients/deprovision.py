# your-analytics-monorepo/airflow/dags/clients/deprovision.py
from __future__ import annotations

import logging
import uuid
import json
import time # For potential sleep or timestamping if needed
from datetime import datetime, timedelta

# Import Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable # Example, if using Airflow Variables
from airflow.hooks.base import BaseHook # To get generic connection details
from airflow.providers.postgres.hooks.postgres import PostgresHook # Hook for PG
from airflow.providers.redis.hooks.redis import RedisHook # Hook for Redis
# No standard hook for ClickHouse native protocol, will use clickhouse-connect directly
import clickhouse_connect

log = logging.getLogger(__name__)

# Define the Connection IDs you configured in the Airflow UI (Admin -> Connections)
POSTGRES_CONN_ID = 'postgres_metadata_conn'
CLICKHOUSE_CONN_ID = 'clickhouse_external_conn' # Assuming this has native protocol details
REDIS_CONN_ID = 'redis_local_conn'
KAFKA_CONN_ID = 'kafka_local_conn' # If needed for Kafka cleanup

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False, # Set to True for production
    'email_on_retry': False, # Set to True for production
    'retries': 1, # Allow retries
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'default_pool',
    # 'priority_weight': 10,
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # Define callbacks for alerting
}

def deprovision_client(**context):
    """
    Deprovision a client from the analytics platform based on client_id
    passed in the DAG run configuration.

    Steps:
    1. Get client_id from DAG run config and validate format
    2. Fetch client details (especially ch_database_name) from PostgreSQL
    3. Update client status in PostgreSQL to 'deprovisioning'
    4. Drop the client's database in ClickHouse
    5. Remove the client's metadata cache entry in Redis
    6. (Optional) Clean up Kafka resources
    7. Update final status to 'deleted' in PostgreSQL

    Args:
        context: Airflow context dictionary containing DAG run configuration

    Raises:
        ValueError: If client_id is missing, invalid, or client not found
        Exception: For other errors during deprovisioning process
    """
    # --- 1. Get and validate client_id from the DAG run configuration ---
    dag_run_conf = context['dag_run'].conf
    client_id = dag_run_conf.get('client_id')
    
    if not client_id:
        log.error("client_id is required in the DAG run configuration.")
        raise ValueError("client_id is required in the DAG run configuration")

    # Validate client_id format
    try:
        uuid.UUID(client_id)
    except ValueError:
        log.error(f"Invalid client_id format: {client_id}")
        raise ValueError(f"Invalid client_id format: {client_id}")

    log.info(f"Starting deprovisioning process for client_id: {client_id}")

    # Initialize hooks and clients
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    ch_client = None
    redis_client = None
    
    try:
        # --- 2. Fetch client details and update status in PostgreSQL ---
        log.info(f"Fetching details for client_id {client_id} from PostgreSQL...")
        with pg_hook.get_conn() as pg_conn:
            with pg_conn.cursor() as cursor:
                try:
                    # Start transaction
                    cursor.execute("BEGIN")
                    
                    # Fetch client details
                    cursor.execute("SELECT client_id, ch_database_name, status FROM clients WHERE client_id = %s;", (client_id,))
                    client_record = cursor.fetchone()

                    if not client_record:
                        cursor.execute("ROLLBACK")
                        log.warning(f"Client with client_id {client_id} not found in PostgreSQL metadata.")
                        raise ValueError(f"Client with client_id {client_id} not found in PostgreSQL metadata.")

                    fetched_client_id, ch_db_name, current_status = client_record
                    log.info(f"Found client {fetched_client_id} with CH DB: {ch_db_name}, Status: {current_status}")

                    # Validate current status
                    if current_status not in ['active', 'pending']:
                        cursor.execute("ROLLBACK")
                        log.warning(f"Client {client_id} is in unexpected status {current_status}")
                        raise ValueError(f"Invalid client status for deprovisioning: {current_status}")

                    # Update status to inactive (instead of deprovisioning)
                    cursor.execute("UPDATE clients SET status = 'inactive' WHERE client_id = %s;", (client_id,))
                    
                    # Commit transaction
                    cursor.execute("COMMIT")
                    log.info(f"Updated client {client_id} status to 'inactive'")
                except Exception as e:
                    cursor.execute("ROLLBACK")
                    raise

        # --- 4. Drop the client's database in ClickHouse ---
        log.info(f"Dropping ClickHouse database: {ch_db_name}")
        
        # Get ClickHouse connection details
        ch_conn_obj = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
        log.info(f"Retrieved ClickHouse connection {CLICKHOUSE_CONN_ID}")
        
        # Extract connection details
        ch_params = {
            'host': ch_conn_obj.host,
            'port': ch_conn_obj.port or 9000,
            'username': ch_conn_obj.login,
            'password': ch_conn_obj.password,
        }
        
        # Handle extra parameters
        if ch_conn_obj.extra:
            try:
                extra_params = ch_conn_obj.extra_dejson
                if 'secure' in extra_params:
                    ch_params['secure'] = extra_params['secure']
            except Exception as e:
                log.warning(f"Could not parse extra parameters from ClickHouse connection: {e}")

        # Connect to ClickHouse
        ch_client = clickhouse_connect.get_client(**ch_params)
        log.info("Successfully connected to ClickHouse")

        try:
            # Check if database exists
            result = ch_client.command(f"SELECT name FROM system.databases WHERE name = '{ch_db_name}'")
            if not result:
                log.warning(f"Database {ch_db_name} does not exist in ClickHouse")
            else:
                ch_client.command(f"DROP DATABASE IF EXISTS `{ch_db_name}`")
                log.info(f"ClickHouse database `{ch_db_name}` dropped successfully.")
        except Exception as e:
            log.error(f"Error checking/dropping ClickHouse database: {e}")
            raise

        # --- 5. Remove the client's metadata cache entry in Redis ---
        log.info(f"Removing Redis cache entry for client_id: {client_id}")
        try:
            redis_hook = RedisHook(redis_conn_id=REDIS_CONN_ID)
            redis_client = redis_hook.get_conn()
            cache_key = f"client:{client_id}"
            deleted_count = redis_client.delete(cache_key)
            log.info(f"Removed {deleted_count} key(s) from Redis cache for client_id {client_id}.")
        except Exception as e:
            log.error(f"Failed to remove Redis cache entry for client_id {client_id}: {e}")
            # Continue with deprovisioning even if Redis cleanup fails

        # --- 7. Final status update in PostgreSQL ---
        log.info(f"Updating final status for client_id {client_id} in PostgreSQL to 'inactive'.")
        pg_hook.run("UPDATE clients SET status = 'inactive' WHERE client_id = %s;", parameters=(client_id,))
        log.info(f"Client {client_id} final status updated to 'inactive'.")

    except ValueError as ve:
        log.error(f"Deprovisioning failed due to configuration or missing client: {ve}", exc_info=True)
        raise
    except Exception as e:
        log.error(f"An error occurred during deprovisioning for client_id {client_id}: {e}", exc_info=True)
        raise
    finally:
        # Cleanup connections
        if ch_client:
            try:
                ch_client.close()
            except Exception as e:
                log.warning(f"Error closing ClickHouse connection: {e}")
        if redis_client:
            try:
                redis_client.close()
            except Exception as e:
                log.warning(f"Error closing Redis connection: {e}")

# Define the DAG instance
with DAG(
    'client_deprovision_dag', # Use a consistent dag_id
    default_args=default_args,
    description='Deprovision a client from the analytics platform',
    schedule=None, # Run manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['clients', 'deprovisioning'],
    params={"client_id": "1234567890"}, # Define expected config parameter
) as dag:

    deprovision_task = PythonOperator(
        task_id='deprovision_client_task', # Use a descriptive task_id
        python_callable=deprovision_client,
        provide_context=True, # Provides context dictionary including dag_run
    )