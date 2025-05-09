# your-analytics-monorepo/airflow/dags/batch/ingest.py
from __future__ import annotations

import logging
import os
# Removed sys, json, tempfile, shutil as they are not used in this version
# import sys
# import json
# import tempfile
# import shutil

from datetime import datetime, timedelta

# Import Airflow modules
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook # To get generic connection details

# Import clickhouse-connect directly
# Ensure clickhouse-connect is in airflow/requirements.txt
import clickhouse_connect

log = logging.getLogger(__name__)

# Define a dummy data source function that returns test data
def generate_dummy_data():
    """Generate some dummy data for testing"""
    # Use datetime objects directly, clickhouse-connect should handle them for DateTime64
    now_utc = datetime.utcnow() 

    users = [
        {"user_id": 1, "name": "Alice", "city": "New York", "signup_time": now_utc},
        {"user_id": 2, "name": "Bob", "city": "Los Angeles", "signup_time": now_utc},
        {"user_id": 3, "name": "Charlie", "city": "Chicago", "signup_time": now_utc},
    ]
    
    page_views = [
        {"view_id": 101, "user_id": 1, "page": "/home", "timestamp": now_utc},
        {"view_id": 102, "user_id": 2, "page": "/products", "timestamp": now_utc},
        {"view_id": 103, "user_id": 1, "page": "/about", "timestamp": now_utc},
    ]
    
    return {"users": users, "page_views": page_views}

# Define the Connection IDs you configured in the Airflow UI (Admin -> Connections)
POSTGRES_CONN_ID = 'postgres_metadata_conn'
CLICKHOUSE_CONN_ID = 'clickhouse_external_conn' # Assuming this has HTTP/HTTPS details

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1, # Allow retries (adjust based on idempotency of load)
    'retry_delay': timedelta(minutes=5),
    # 'execution_timeout': timedelta(hours=1), # Timeout for batch run
    # 'on_failure_callback': some_alerting_function,
}

# --- Define the DAG ---
@dag(
    dag_id='batch_ingestion_dag',
    default_args=default_args,
    description='Runs data ingestion to load batch data for active clients into ClickHouse.',
    schedule=None, # Set a schedule like '@daily' (UTC) for daily batches, or leave as None for manual runs
    start_date=datetime(2023, 1, 1), # Set a historical start date
    catchup=False, # Don't run for past missed schedules
    tags=['batch', 'ingestion', 'clickhouse'],
)
def batch_ingestion_pipeline():

    @task
    def fetch_active_clients_for_batch(**context):
        """
        Fetches active clients from PostgreSQL metadata database for batch ingestion.
        Returns a list of tuples: [(client_id_str, ch_database_name_str)].
        """
        log.info("Fetching active clients for batch ingestion from PostgreSQL...")
        
        try:
            # Use PostgresHook and the connection ID configured in Airflow UI
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

            # Get a connection from the pool managed by the hook
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # Select active clients
                    cursor.execute("SELECT client_id, ch_database_name FROM clients WHERE status = 'active';")
                    active_clients = cursor.fetchall()
                    log.info(f"Found {len(active_clients)} active clients for batch ingestion.")

                    # Return the list of pending clients as tuples (UUID object, str)
                    # Convert UUID to string for easier handling in downstream tasks/XCom
                    return [(str(client[0]), client[1]) for client in active_clients] # Return as list of (str, str) tuples

        except Exception as e:
            log.error(f"Error fetching active clients for batch ingestion: {e}", exc_info=True)
            # Re-raise to fail the task
            raise


    @task
    def run_ingestion_for_client(client_info: tuple, **context):
        """
        Runs data ingestion for a single client using ClickHouse directly via clickhouse-connect.
        Creates tables if they don't exist and inserts dummy data.
        """
        client_id, ch_db_name = client_info
        log.info(f"Running data ingestion for client {client_id} into DB `{ch_db_name}`.")

        client = None # Initialize client variable
        try:
            # Get ClickHouse connection details from Airflow Connection
            ch_conn_obj = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
            
            # Extract connection details for clickhouse-connect
            # clickhouse-connect defaults to HTTP/HTTPS, so assuming HTTP/HTTPS on configured port
            host = ch_conn_obj.host
            # Use the port from the Airflow Connection object.
            # If your Airflow Connection uses 9000 (Native) but HTTP is on 8123,
            # update your Airflow Connection to use 8123 if you want to use HTTP.
            port = ch_conn_obj.port 
            username = ch_conn_obj.login
            password = ch_conn_obj.password
            # Database is handled during connection or in commands
            
            log.info(f"Connecting to ClickHouse at {host}:{port} as {username} for database {ch_db_name}")
            
            # Create ClickHouse client using clickhouse-connect
            # It will use HTTP/HTTPS based on the port or protocol in extra.
            try:
                # Parse the extra parameters if they exist
                protocol = None
                secure = None
                if ch_conn_obj.extra:
                    try:
                        extra_params = ch_conn_obj.extra_dejson
                        protocol = extra_params.get('protocol')
                        secure = extra_params.get('secure')
                    except Exception as e:
                        log.warning(f"Could not parse extra parameters from ClickHouse connection: {e}")
                
                # If protocol is not explicitly set, determine it from the port
                if not protocol:
                    if port == 8123:
                        protocol = 'http'
                        secure = False
                    elif port == 8443:
                        protocol = 'https'
                        secure = True
                    elif port == 9000:
                        protocol = 'native'
                        secure = False
                    elif port == 9440:
                        protocol = 'native'
                        secure = True
                
                # Log the connection parameters for debugging
                log.info(f"Connecting with secure={secure}")
                
                client = clickhouse_connect.get_client(
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    database=ch_db_name,
                    secure=secure,
                    connect_timeout=10,
                    query_limit=0,
                    compress=False
                )
            except Exception as e:
                log.error(f"Error creating ClickHouse client: {e}", exc_info=True)
                raise
            
            # Test the connection by running a simple command in the client's database
            # Use command() for non-SELECT statements and simple queries
            client.command('SELECT 1') # Basic connectivity test after selecting DB
            log.info("Successfully connected to ClickHouse and selected database.")
            
            # Generate dummy data
            data = generate_dummy_data()
            log.info(f"Generated dummy data with {len(data['users'])} users and {len(data['page_views'])} page views")
            
            # Create tables if they don't exist
            # Use client.command() for DDL statements (CREATE TABLE)
            log.info("Creating tables if not exists...")
            client.command(f"""
                CREATE TABLE IF NOT EXISTS `{ch_db_name}`.users (
                    user_id UInt32,
                    name String,
                    city String,
                    signup_time DateTime64(3)
                ) ENGINE = MergeTree()
                ORDER BY (user_id)
            """)
            
            client.command(f"""
                CREATE TABLE IF NOT EXISTS `{ch_db_name}`.page_views (
                    view_id UInt32,
                    user_id UInt32,
                    page String,
                    timestamp DateTime64(3)
                ) ENGINE = MergeTree()
                ORDER BY (view_id, user_id)
            """)
            log.info("Tables creation attempted.")
            
            # Prepare data for insertion
            # clickhouse-connect.insert expects a list of lists or list of dicts
            users_rows = []
            for user in data['users']:
                users_rows.append([
                    user['user_id'],
                    user['name'],
                    user['city'],
                    # Pass datetime objects directly to insert for DateTime64
                    user['signup_time'] 
                ])
            
            page_views_rows = []
            for view in data['page_views']:
                page_views_rows.append([
                    view['view_id'],
                    view['user_id'],
                    view['page'],
                    # Pass datetime objects directly to insert for DateTime64
                    view['timestamp']
                ])
            
            # Perform the inserts using client.insert()
            # Specify the database and table name in the insert call
            log.info(f"Inserting {len(users_rows)} rows into `{ch_db_name}`.users...")
            try:
                user_insert_result = client.insert(
                    table='users',
                    data=users_rows,
                    column_names=['user_id', 'name', 'city', 'signup_time'],
                    database=ch_db_name
                )
                # Insert result handling depends on clickhouse-connect version
                if hasattr(user_insert_result, 'written_rows'):
                    log.info(f"Inserted {user_insert_result.written_rows} rows into `{ch_db_name}`.users table.")
                else:
                    log.info(f"Inserted {len(users_rows)} rows into `{ch_db_name}`.users table.")
            except Exception as e:
                log.error(f"Failed to insert into users table: {e}", exc_info=True)
                raise
            
            log.info(f"Inserting {len(page_views_rows)} rows into `{ch_db_name}`.page_views...")
            try:
                page_view_insert_result = client.insert(
                    table='page_views',
                    data=page_views_rows,
                    column_names=['view_id', 'user_id', 'page', 'timestamp'],
                    database=ch_db_name
                )
                # Insert result handling depends on clickhouse-connect version
                if hasattr(page_view_insert_result, 'written_rows'):
                    log.info(f"Inserted {page_view_insert_result.written_rows} rows into `{ch_db_name}`.page_views table.")
                else:
                    log.info(f"Inserted {len(page_views_rows)} rows into `{ch_db_name}`.page_views table.")
            except Exception as e:
                log.error(f"Failed to insert into page_views table: {e}", exc_info=True)
                raise
            
            # Verify data loaded using client.query() instead of command() for SELECT queries
            try:
                user_count_result = client.query(f"SELECT count() FROM `{ch_db_name}`.users")
                page_view_count_result = client.query(f"SELECT count() FROM `{ch_db_name}`.page_views")
                
                # Extract the count from the result
                user_count = user_count_result.result_rows[0][0] if user_count_result.result_rows else 0
                page_view_count = page_view_count_result.result_rows[0][0] if page_view_count_result.result_rows else 0
                
                log.info(f"Verification: {user_count} total users, {page_view_count} total page_views in database `{ch_db_name}`")
            except Exception as e:
                log.error(f"Failed to verify data counts: {e}", exc_info=True)
                # Don't raise here, just log the error and continue
            
            # Return summary of the operation
            result = {
                'client_id': client_id,
                'database': ch_db_name,
                'tables_loaded': ['users', 'page_views'],
                'rows_loaded': {
                    'users': len(data['users']),
                    'page_views': len(data['page_views'])
                },
                'total_rows': len(data['users']) + len(data['page_views']),
                'timestamp_utc': datetime.utcnow().isoformat()
            }
            
            log.info(f"Data ingestion completed successfully for client {client_id}")
            return result
            
        except Exception as e:
            log.error(f"Error running data ingestion for client {client_id}: {e}", exc_info=True)
            raise # Re-raise to make the Airflow task fail
        finally:
            # Explicitly close the connection obtained from get_client
            if client:
                try: client.close()
                except Exception: pass # Ignore errors during close


    # --- Define the task flow ---
    # 1. Fetch all active clients
    active_clients = fetch_active_clients_for_batch()

    # 2. For each active client, run the data ingestion
    # Use expand to create a task instance for each active client
    run_ingestion_for_client.expand(client_info=active_clients)


# Instantiate the DAG
batch_ingestion_pipeline()

# --- END OF DAG FILE ---
