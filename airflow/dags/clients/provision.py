# your-analytics-monorepo/airflow/dags/clients/provision.py
from __future__ import annotations

import logging
import uuid
import json # Needed for json.dumps if serializing complex data
import time # Needed for default timestamps if generating them

from datetime import datetime

# Import necessary Airflow modules
from airflow.decorators import dag, task # Using TaskFlow API
from airflow.providers.postgres.hooks.postgres import PostgresHook # Standard Hook for PG

# We will use clickhouse-connect for ClickHouse interaction
# Ensure clickhouse-connect is in airflow/requirements.txt
import clickhouse_connect
# We might need os for environment variables if not using Airflow Connections
import os

log = logging.getLogger(__name__)

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False, # Set to True for production
    'email_on_retry': False, # Set to True for production
    'retries': 1, # Allow retries for potentially transient issues
    'retry_delay': 5, # minutes
    # 'queue': 'bash_queue',
    # 'pool': 'default_pool',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2, minutes=0),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # Define callbacks for alerting
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# --- Get connection details from Airflow Connections ---
# This is the PREFERRED method using Hook and Connection IDs
# Ensure you have created these Connections in the Airflow UI (Admin -> Connections)
POSTGRES_CONN_ID = 'postgres_metadata_conn'
CLICKHOUSE_CONN_ID = 'clickhouse_external_conn'
# Add other connection IDs if needed (e.g., redis_local_conn, kafka_local_conn)

# --- Define the DAG using the decorator ---
@dag(
    dag_id='client_provisioning_dag',
    default_args=default_args,
    description='Provisions new clients by creating resources in PG and ClickHouse.',
    schedule=None, # Set a schedule (e.g., '@daily', '0 0 * * *') or None for manual runs
    start_date=datetime(2023, 1, 1), # Set a historical start date
    catchup=False, # Don't run for past missed schedules
    tags=['client', 'provisioning', 'setup'],
)
def client_provisioning_pipeline(): # This function defines the DAG's structure and tasks

    @task
    def fetch_pending_clients(**context):
        """
        Fetches clients from PostgreSQL with status 'pending' using PostgresHook.
        Returns a list of tuples: [(client_id_str, ch_database_name_str)].
        """
        log.info("Fetching pending clients from PostgreSQL...")
        
        try:
            # Use PostgresHook and the connection ID configured in Airflow UI
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            
            # Get a connection from the pool managed by the hook
            # Using 'with' statement ensures connection is returned to pool
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # Select clients where status is 'pending'
                    # Assuming client_id is UUID and ch_database_name is TEXT
                    cursor.execute("SELECT client_id, ch_database_name FROM clients WHERE status = 'pending';")
                    pending_clients = cursor.fetchall()
                    log.info(f"Found {len(pending_clients)} pending clients.")
                    
                    # Return the list of pending clients as tuples (UUID object, str)
                    # Convert UUID to string for easier handling in downstream tasks/XCom
                    # XCom automatically serializes/deserializes JSON-compatible types
                    return [(str(client[0]), client[1]) for client in pending_clients] # Return as list of (str, str) tuples

        except Exception as e:
            log.error(f"Error fetching pending clients: {e}", exc_info=True)
            # Re-raise to fail the task
            raise


    @task
    def create_clickhouse_db_and_schema(client_info: tuple, **context):
        """
        Creates the ClickHouse database and applies baseline schema for a client
        using clickhouse-connect via Airflow Connection.
        Returns the input tuple if successful to pass to the next task.
        """
        # Unpack the tuple received from the upstream task
        client_id, ch_db_name = client_info 
        log.info(f"Processing client {client_id}: Creating ClickHouse DB {ch_db_name} and schema.")

        # Get ClickHouse connection details from Airflow Connection
        from airflow.models import Connection
        from airflow.hooks.base import BaseHook

        try:
            # Get the connection object from Airflow
            conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
            
            # Extract connection details
            ch_params = {
                'host': conn.host,
                'port': conn.port if conn.port else 9000,  # Use port from connection, fallback to 9000
                'username': conn.login,
                'password': conn.password,
            }
            
            # Add any extra parameters from the connection
            if conn.extra:
                try:
                    extra_params = conn.extra_dejson
                    if 'secure' in extra_params:
                        ch_params['secure'] = extra_params['secure']
                except Exception as e:
                    log.warning(f"Could not parse extra parameters from connection: {e}")

            log.info(f"Using ClickHouse connection details: {ch_params}")

            client = None
            try:
                # Connect to ClickHouse using the connection parameters
                client = clickhouse_connect.get_client(**ch_params)

                # 1. Create the database for the client
                log.info(f"Creating database `{ch_db_name}`...")
                client.command(f"CREATE DATABASE IF NOT EXISTS `{ch_db_name}`")
                log.info(f"Database `{ch_db_name}` created (if not exists).")

                # 2. Apply baseline schema (events_base table)
                schema_file_path = '/opt/airflow/dags/clickhouse/schemas/01_events_base.sql' 
                try:
                    with open(schema_file_path, 'r') as f:
                        schema_sql = f.read()
                    log.info(f"Applying schema from {schema_file_path} to `{ch_db_name}`...")

                    # Execute the SQL command within the client's database context
                    client.command(f"USE `{ch_db_name}`; {schema_sql}")

                    log.info(f"Baseline schema applied to `{ch_db_name}`.")

                    # Optional: Apply materialized view schema if it's part of baseline
                    mv_schema_file_path = '/opt/airflow/dags/clickhouse/schemas/02_events_aggregated_view.sql'
                    try:
                        if os.path.exists(mv_schema_file_path):
                            with open(mv_schema_file_path, 'r') as f:
                                mv_schema_sql = f.read()
                            log.info(f"Applying MV schema from {mv_schema_file_path} to `{ch_db_name}`...")
                            client.command(f"USE `{ch_db_name}`; {mv_schema_sql}")
                            log.info(f"Materialized view schema applied to `{ch_db_name}`.")
                        else:
                            log.warning(f"Materialized view schema file not found at expected path: {mv_schema_file_path}")
                    except Exception as e:
                        log.error(f"Error applying MV schema to `{ch_db_name}`: {e}", exc_info=True)
                        # Continue if MV fails but base table succeeds

                except FileNotFoundError:
                    log.error(f"Schema file not found at expected path: {schema_file_path}", exc_info=True)
                    raise # Fail the task if schema file is missing
                except Exception as e:
                    log.error(f"Error applying schema to `{ch_db_name}`: {e}", exc_info=True)
                    raise # Fail the task on schema application error

                # Return the input client info tuple to pass to the next task
                return client_info

            except Exception as e:
                log.error(f"Error connecting to ClickHouse or creating DB for client {client_id}: {e}", exc_info=True)
                raise
            finally:
                # Explicitly close the connection obtained from get_client
                if client:
                    try: client.close()
                    except Exception: pass # Ignore errors during close

        except Exception as e:
            log.error(f"Error getting ClickHouse connection details: {e}", exc_info=True)
            raise


    @task
    def update_client_status(client_info: tuple, **context):
        """
        Updates the client status in PostgreSQL to 'active' using PostgresHook.
        Requires the client_info tuple (client_id_str, ch_database_name_str).
        """
        # Unpack the tuple received from the upstream task
        client_id, ch_db_name = client_info 
        log.info(f"Updating status for client {client_id} in PG...")

        try:
            # Use PostgresHook and the connection ID configured in Airflow UI
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            
            # Get a connection from the pool managed by the hook
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # Update status to 'active' for the client_id
                    # Use %s placeholder and pass client_id as a tuple
                    cursor.execute("UPDATE clients SET status = 'active' WHERE client_id = %s;", (client_id,)) 
                    conn.commit() # Commit the transaction
                    log.info(f"Client {client_id} status updated to 'active'.")
        except Exception as e:
            log.error(f"Error updating client status for {client_id}: {e}", exc_info=True)
            # PostgresHook connections might handle rollback automatically in some cases,
            # but explicit rollback on error is good practice if managing transactions manually.
            # If using the Hook's run method, it handles transactions.
            raise # Re-raise to fail the task


    # --- Define the task flow (dependencies) ---
    # This section defines the order and data passing between tasks.
    
    # 1. Fetch pending clients. This returns a list of clients.
    pending_clients = fetch_pending_clients()

    # 2. For each client returned by fetch_pending_clients, run the create_clickhouse_db_and_schema task.
    # `expand` takes the list and creates a separate task instance for each item.
    # The output of each instance (the client_info tuple) is collected into a new list.
    created_clients = create_clickhouse_db_and_schema.expand(client_info=pending_clients)

    # 3. For each client that had its DB/schema created, run the update_client_status task.
    # `expand` takes the list of client_info tuples from the previous step.
    update_client_status.expand(client_info=created_clients)


# --- Instantiate the DAG ---
client_provisioning_pipeline()


# You will create similar files for other DAGs:
# airflow/dags/clients/deprovision.py
# airflow/dags/schema/migrate.py
# airflow/dags/batch/ingest.py
# airflow/dags/realtime/dlq_reprocess.py