# your-analytics-monorepo/airflow/dags/schema/migrate.py
from __future__ import annotations

import logging
import os
import re # For sorting migration files

from datetime import datetime, timedelta

# Import Airflow modules
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook  # Add missing import
# We use clickhouse-connect directly as before
import clickhouse_connect

log = logging.getLogger(__name__)

# Define the Connection IDs you configured in the Airflow UI
POSTGRES_CONN_ID = 'postgres_metadata_conn'
CLICKHOUSE_CONN_ID = 'clickhouse_external_conn'

# Define the path to the migration files relative to the DAGs folder mount
MIGRATIONS_DIR_INSIDE_CONTAINER = '/opt/airflow/dags/clickhouse/migrations/'

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True, # Enable email on failure for migrations!
    'email_on_retry': False,
    'retries': 1, # Migrations are often not safe to retry automatically without careful idempotency
    'retry_delay': timedelta(minutes=5),
    # 'execution_timeout': timedelta(seconds=600), # Set a timeout for long migrations
    # 'on_failure_callback': some_alerting_function,
}

# --- Helper function to get ClickHouse client from Airflow Connection ---
def get_ch_client_from_conn(conn_id: str):
    """Gets a clickhouse-connect client from an Airflow Connection ID."""
    try:
        # Get the connection object from Airflow
        conn = BaseHook.get_connection(conn_id)
        
        # Extract connection details for clickhouse-connect
        ch_params = {
            'host': conn.host,
            'port': conn.port or 9000, # Default to 9000 if port is None in conn
            'username': conn.login,
            'password': conn.password,
        }
        
        # Handle extra parameters like 'secure'
        if conn.extra:
            try:
                extra_params = conn.extra_dejson
                if 'secure' in extra_params:
                    ch_params['secure'] = extra_params['secure']
                # Add other extra parameters if needed by clickhouse-connect
            except Exception as e:
                log.warning(f"Could not parse extra parameters from ClickHouse connection: {e}")

        # Connect to ClickHouse
        log.info(f"Connecting to ClickHouse with parameters (excluding password): Host={ch_params.get('host')}, Port={ch_params.get('port')}, User={ch_params.get('username')}, Secure={ch_params.get('secure')}")
        client = clickhouse_connect.get_client(**ch_params)
        log.info("Successfully connected to ClickHouse")
        return client

    except Exception as e:
        log.error(f"Error getting ClickHouse connection details or client: {e}", exc_info=True)
        raise # Re-raise to fail the task

# --- Define the DAG ---
@dag(
    dag_id='schema_migration_dag',
    default_args=default_args,
    description='Applies schema migrations to client databases in ClickHouse.',
    schedule=None, # Run manually after deployments, or define a specific schedule
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['schema', 'migration'],
)
def schema_migration_pipeline():

    @task
    def fetch_active_clients(**context):
        """
        Fetches active clients from PostgreSQL metadata database.
        Returns a list of tuples: [(client_id_str, ch_database_name_str)].
        """
        log.info("Fetching active clients from PostgreSQL...")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        try:
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # Select active clients
                    cursor.execute("SELECT client_id, ch_database_name FROM clients WHERE status = 'active';")
                    active_clients = cursor.fetchall()
                    log.info(f"Found {len(active_clients)} active clients.")
                    # Return as list of (str, str) tuples
                    return [(str(client[0]), client[1]) for client in active_clients]

        except Exception as e:
            log.error(f"Error fetching active clients: {e}", exc_info=True)
            raise # Re-raise to fail the task


    @task
    def apply_migrations_for_client(client_info: tuple, **context):
        """
        Applies pending schema migration scripts to a single client's database.
        Reads migration files, checks schema_versions in PG, and executes pending ones.
        """
        client_id, ch_db_name = client_info
        log.info(f"Applying migrations for client {client_id} to DB `{ch_db_name}`.")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        ch_client = None

        try:
            # 1. Get applied migrations for this client from PostgreSQL
            applied_migrations = set()
            try:
                with pg_hook.get_conn() as pg_conn:
                    with pg_conn.cursor() as cursor:
                        # Query the schema_versions table in the metadata DB
                        cursor.execute("SELECT migration_id FROM schema_versions WHERE client_id = %s ORDER BY applied_at;", (client_id,))
                        applied_migrations = {row[0] for row in cursor.fetchall()}
                        log.info(f"Client {client_id} has applied migrations: {applied_migrations}")
            except Exception as e:
                log.error(f"Error fetching applied migrations for client {client_id}: {e}", exc_info=True)
                raise

            # 2. Discover available migration files
            available_migrations = []
            try:
                # List files in the mounted migration directory
                migration_files = os.listdir(MIGRATIONS_DIR_INSIDE_CONTAINER)
                # Filter for SQL files and sort them version by version (e.g., v1_..., v2_...)
                sql_files = sorted([f for f in migration_files if f.endswith('.sql')])

                # Simple regex to extract version number (e.g., v1, v2) for ordering
                # Assumes filenames are like vX_description.sql
                def sort_key(filename):
                    match = re.match(r'v(\d+)_.*\.sql', filename)
                    return int(match.group(1)) if match else float('inf') # Sort by version number

                available_migrations = sorted(sql_files, key=sort_key)

                log.info(f"Available migration files: {available_migrations}")
            except FileNotFoundError:
                log.error(f"Migration directory not found: {MIGRATIONS_DIR_INSIDE_CONTAINER}", exc_info=True)
                raise # Migration files are necessary
            except Exception as e:
                log.error(f"Error discovering migration files: {e}", exc_info=True)
                raise # Failed to read migration files

            # 3. Determine pending migrations
            pending_migrations = [
                migration_file for migration_file in available_migrations
                if migration_file not in applied_migrations
            ]

            if not pending_migrations:
                log.info(f"No pending migrations for client {client_id}.")
                return # Exit task successfully if no pending migrations

            log.info(f"Pending migrations for client {client_id}: {pending_migrations}")

            # 4. Connect to ClickHouse for the client's database
            ch_client = get_ch_client_from_conn(CLICKHOUSE_CONN_ID)

            # 5. Apply pending migrations sequentially
            applied_this_run = []
            for migration_file in pending_migrations:
                migration_id = migration_file # Use filename as migration ID
                migration_path = os.path.join(MIGRATIONS_DIR_INSIDE_CONTAINER, migration_file)
                log.info(f"Applying migration {migration_id} to `{ch_db_name}`...")

                try:
                    # Read and validate migration SQL
                    with open(migration_path, 'r') as f:
                        migration_sql = f.read().strip()
                    
                    if not migration_sql:
                        raise ValueError(f"Migration file {migration_id} is empty")

                    # Execute the SQL script against the client's database
                    # First set the database context
                    ch_client.command(f"USE `{ch_db_name}`")
                    # Then execute the migration
                    ch_client.command(migration_sql)

                    log.info(f"Successfully applied migration {migration_id} to `{ch_db_name}`.")
                    applied_this_run.append(migration_id)

                except FileNotFoundError:
                    log.error(f"Migration file disappeared during execution: {migration_path}", exc_info=True)
                    raise # Critical error
                except Exception as e:
                    log.error(f"Error applying migration {migration_id} to `{ch_db_name}`: {e}", exc_info=True)
                    raise # Fail the task

            # 6. Record applied migrations in PostgreSQL metadata DB
            if applied_this_run:
                log.info(f"Recording applied migrations for client {client_id}: {applied_this_run}")
                try:
                    with pg_hook.get_conn() as pg_conn:
                        with pg_conn.cursor() as cursor:
                            try:
                                # Start transaction
                                cursor.execute("BEGIN")
                                
                                # Insert records into schema_versions table
                                insert_sql = "INSERT INTO schema_versions (client_id, migration_id, applied_at) VALUES (%s, %s, NOW());"
                                values = [(client_id, mig_id) for mig_id in applied_this_run]
                                cursor.executemany(insert_sql, values)
                                
                                # Commit transaction
                                cursor.execute("COMMIT")
                                log.info(f"Successfully recorded {len(applied_this_run)} applied migrations for client {client_id}.")
                            except Exception as e:
                                cursor.execute("ROLLBACK")
                                raise
                except Exception as e:
                    log.error(f"Error recording applied migrations for client {client_id}: {e}", exc_info=True)
                    raise


        except Exception as e:
            # Catch any exceptions that weren't specifically handled above
            log.error(f"An error occurred during migration for client {client_id}: {e}", exc_info=True)
            raise # Re-raise to fail the task

        finally:
            # Explicitly close ClickHouse connection if it was opened
            if ch_client:
                try: 
                    ch_client.close()
                    log.info("Closed ClickHouse connection")
                except Exception as e:
                    log.warning(f"Error closing ClickHouse connection: {e}")


    # --- Define the task flow ---
    # 1. Fetch all active clients
    active_clients = fetch_active_clients()

    # 2. Apply migrations for each active client in parallel
    # Use expand to create a task instance for each active client
    apply_migrations_for_client.expand(client_info=active_clients)


# Instantiate the DAG
schema_migration_pipeline()