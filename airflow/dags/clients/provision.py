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
                    clients = [(str(client[0]), client[1]) for client in pending_clients] # Return as list of (str, str) tuples
                    log.info(f"Returning clients: {clients}")
                    return clients

        except Exception as e:
            log.error(f"Error fetching pending clients: {e}", exc_info=True)
            # Re-raise to fail the task
            raise


    @task(retries=2)
    def create_clickhouse_db_and_schema(client_info, **context):
        """
        Creates the ClickHouse database and applies baseline schema for a client
        using clickhouse-connect via Airflow Connection.
        Returns the input tuple if successful to pass to the next task.
        """
        # Unpack the tuple received from the upstream task
        log.info(f"Client info: {client_info}")
        
        # Handle different input formats safely
        if isinstance(client_info, list):
            # This handles when client_info is a list containing one tuple
            if len(client_info) > 0:
                # Unpack if it's a single-element list containing a tuple
                if isinstance(client_info[0], tuple) and len(client_info[0]) == 2:
                    client_id, ch_db_name = client_info[0]
                # Or if it's just a list with two elements
                elif len(client_info) == 2:
                    client_id, ch_db_name = client_info
                else:
                    log.error(f"Unexpected client_info format: {client_info}")
                    raise ValueError(f"Invalid client_info format: {client_info}")
            else:
                log.error("Empty client_info list received")
                raise ValueError("Empty client_info list received")
        elif isinstance(client_info, tuple) and len(client_info) == 2:
            # Direct tuple input
            client_id, ch_db_name = client_info
        else:
            log.error(f"Unexpected client_info type: {type(client_info)}, value: {client_info}")
            raise ValueError(f"Invalid client_info format: {client_info}")
            
        log.info(f"Processing client {client_id}: Creating ClickHouse DB `{ch_db_name}` and schema.")

        # Get ClickHouse connection details from Airflow Connection
        from airflow.models import Connection
        from airflow.hooks.base import BaseHook

        try:
            log.info(f"Attempting to retrieve connection: {CLICKHOUSE_CONN_ID}")
            conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
            log.info(f"Successfully retrieved connection object for {CLICKHOUSE_CONN_ID}")
            
            # Extract connection details
            ch_params = {
                'host': conn.host,
                'port': conn.port,
                'username': conn.login,
                'password': conn.password,
                # Add default secure to False if not specified, adjust as needed
                'secure': False, 
            }
            log.info(f"Base connection parameters extracted: host={conn.host}, port={conn.port}, user={conn.login is not None}") # Avoid logging password
            
            # Add any extra parameters from the connection
            if conn.extra:
                log.info(f"Parsing extra connection parameters: {conn.extra}")
                try:
                    extra_params = conn.extra_dejson
                    # Include all the relevant parameters
                    # Note: clickhouse-connect uses 'secure' for https, not protocol
                    if 'secure' in extra_params:
                        ch_params['secure'] = extra_params['secure']
                        log.info(f"Set secure={ch_params['secure']} from extra params")
                    if 'verify' in extra_params:
                        ch_params['verify'] = extra_params['verify']
                        log.info(f"Set verify={ch_params['verify']} from extra params")
                    if 'connect_timeout' in extra_params:
                        ch_params['connect_timeout'] = extra_params['connect_timeout']
                        log.info(f"Set connect_timeout={ch_params['connect_timeout']} from extra params")
                    if 'send_receive_timeout' in extra_params:
                        ch_params['send_receive_timeout'] = extra_params['send_receive_timeout']
                        log.info(f"Set send_receive_timeout={ch_params['send_receive_timeout']} from extra params")
                    if 'client_name' in extra_params:
                        ch_params['client_name'] = extra_params['client_name']
                        log.info(f"Set client_name='{ch_params['client_name']}' from extra params")
                    # Remove protocol if present, as it's inferred from 'secure' by the library
                    if 'protocol' in extra_params:
                        log.warning("Ignoring 'protocol' in extra params, use 'secure' for HTTPS.")
                        
                except Exception as e:
                    log.warning(f"Could not parse extra parameters from connection: {e}", exc_info=True)

            log.info(f"Final ClickHouse connection parameters (password omitted): {{k: v for k, v in ch_params.items() if k != 'password'}}")

            # Remove protocol explicitly if it somehow got added, ensure secure param is used
            if 'protocol' in ch_params:
                log.warning("Removing unexpected 'protocol' parameter. Use 'secure=True' for HTTPS.")
                del ch_params['protocol']  # Remove this as it's not used by clickhouse-connect

            client = None
            try:
                # Connect to ClickHouse using the connection parameters
                log.info(f"Attempting to connect to ClickHouse server at {ch_params.get('host')}:{ch_params.get('port')}")
                client = clickhouse_connect.get_client(**ch_params)
                log.info(f"Successfully connected to ClickHouse server.")
                client.ping() # Verify connection is alive
                log.info("ClickHouse connection ping successful.")

                # 1. Create the database for the client
                db_create_sql = f"CREATE DATABASE IF NOT EXISTS `{ch_db_name}`"
                log.info(f"Executing DB creation: {db_create_sql}")
                client.command(db_create_sql)
                log.info(f"Database `{ch_db_name}` created or already exists.")

                # 2. Apply baseline schema (events_base table)
                schema_file_path = '/opt/airflow/dags/clickhouse/schemas/01_events_base.sql' 
                log.info(f"Attempting to read base schema file: {schema_file_path}")
                try:
                    with open(schema_file_path, 'r') as f:
                        schema_sql = f.read()
                    log.info(f"Successfully read {len(schema_sql)} bytes from {schema_file_path}.")
                    log.info(f"Applying base schema to `{ch_db_name}`...")

                    # Instead, execute statements separately:
                    log.info(f"Setting database context to `{ch_db_name}`...")
                    client.command(f"USE `{ch_db_name}`")
                    
                    log.info(f"Executing base schema SQL (first 50 chars): {schema_sql[:50]}...")
                    client.command(schema_sql)

                    log.info(f"Baseline schema applied successfully to `{ch_db_name}`.")

                    # Optional: Apply materialized view schema if it's part of baseline
                    mv_schema_file_path = '/opt/airflow/dags/clickhouse/schemas/02_events_aggregated_view.sql'
                    log.info(f"Checking for MV schema file: {mv_schema_file_path}")
                    try:
                        if os.path.exists(mv_schema_file_path):
                            log.info(f"MV schema file found. Reading...")
                            with open(mv_schema_file_path, 'r') as f:
                                mv_schema_sql = f.read()
                            log.info(f"Successfully read {len(mv_schema_sql)} bytes from {mv_schema_file_path}.")
                            log.info(f"Applying MV schema to `{ch_db_name}`...")
                            
                            # Instead, execute statements separately:
                            log.info(f"Setting database context to `{ch_db_name}` for MV creation...")
                            client.command(f"USE `{ch_db_name}`")
                            
                            log.info(f"Executing MV schema SQL (first 50 chars): {mv_schema_sql[:50]}...")
                            client.command(mv_schema_sql)
                            
                            log.info(f"Materialized view schema applied successfully to `{ch_db_name}`.")
                        else:
                            log.warning(f"Materialized view schema file not found, skipping: {mv_schema_file_path}")
                    except Exception as e_mv:
                        log.error(f"Error applying MV schema to `{ch_db_name}`: {e_mv}", exc_info=True)
                        # Decide if this is fatal or not. Currently continues.
                        log.warning("Continuing DAG execution despite MV schema application error.")

                except FileNotFoundError:
                    log.error(f"CRITICAL: Base schema file not found at expected path: {schema_file_path}", exc_info=True)
                    raise # Fail the task if base schema file is missing
                except Exception as e_schema:
                    log.error(f"CRITICAL: Error applying base schema to `{ch_db_name}`: {e_schema}", exc_info=True)
                    raise # Fail the task on base schema application error

                log.info(f"ClickHouse provisioning steps completed for client {client_id}, database {ch_db_name}.")
                # Return the input client info tuple to pass to the next task
                return client_info

            except Exception as e_connect_db:
                # Catch specific connection/command errors if possible
                log.error(f"CRITICAL: Error during ClickHouse connection or DB/Schema setup for client {client_id}: {e_connect_db}", exc_info=True)
                raise # Re-raise the exception to fail the task
            finally:
                # Explicitly close the connection obtained from get_client
                if client:
                    log.info("Attempting to close ClickHouse client connection.")
                    try: 
                        client.close()
                        log.info("ClickHouse client connection closed.")
                    except Exception as e_close: 
                         log.warning(f"Ignoring error during ClickHouse client close: {e_close}", exc_info=True)

        except Exception as e_get_conn:
            log.error(f"CRITICAL: Error getting ClickHouse connection details from Airflow: {e_get_conn}", exc_info=True)
            raise # Re-raise to fail the task


    @task
    def update_client_status(client_info, **context):
        """
        Updates the client status in PostgreSQL to 'active' using PostgresHook.
        Requires the client_info tuple (client_id_str, ch_database_name_str).
        """
        # Unpack the tuple received from the upstream task
        log.info(f"Client info for status update: {client_info}")
        
        # Handle different input formats safely - use same logic as in create_clickhouse_db_and_schema
        if isinstance(client_info, list):
            # This handles when client_info is a list containing one tuple
            if len(client_info) > 0:
                # Unpack if it's a single-element list containing a tuple
                if isinstance(client_info[0], tuple) and len(client_info[0]) == 2:
                    client_id, ch_db_name = client_info[0]
                # Or if it's just a list with two elements
                elif len(client_info) == 2:
                    client_id, ch_db_name = client_info
                else:
                    log.error(f"Unexpected client_info format: {client_info}")
                    raise ValueError(f"Invalid client_info format: {client_info}")
            else:
                log.error("Empty client_info list received")
                raise ValueError("Empty client_info list received")
        elif isinstance(client_info, tuple) and len(client_info) == 2:
            # Direct tuple input
            client_id, ch_db_name = client_info
        else:
            log.error(f"Unexpected client_info type: {type(client_info)}, value: {client_info}")
            raise ValueError(f"Invalid client_info format: {client_info}")
            
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
    # Create a non-mapped version for testing if pending_clients is empty
    if not pending_clients:
        # Create a task for the test client in case no pending clients are found
        test_client = ('test-client-id', 'test_client_db')
        test_created = create_clickhouse_db_and_schema(test_client)
        test_updated = update_client_status(test_created)
    else:
        # Use expand for the normal flow with real clients
        created_clients = create_clickhouse_db_and_schema.expand(client_info=pending_clients)
        update_client_status.expand(client_info=created_clients)


# --- Instantiate the DAG ---
client_provisioning_pipeline()


# You will create similar files for other DAGs:
# airflow/dags/clients/deprovision.py
# airflow/dags/schema/migrate.py
# airflow/dags/batch/ingest.py
# airflow/dags/realtime/dlq_reprocess.py