# your-analytics-monorepo/airflow/dags/realtime/dlq_reprocess.py
from __future__ import annotations

import logging
import json
import time
import uuid # Needed for generating UUIDs if event_id is missing
import hashlib # Needed if hashing API keys in reprocessing
from datetime import datetime, timedelta

# Import Airflow modules
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook # To get generic connection details
from airflow.providers.redis.hooks.redis import RedisHook # Hook for Redis
from airflow.providers.postgres.hooks.postgres import PostgresHook # Hook for PG
from airflow.models import Variable # For accessing Airflow Variables

# Import KafkaConsumer and KafkaProducer from kafka-python
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.structs import OffsetAndMetadata
from kafka.errors import KafkaError

# Import clickhouse-connect directly
import clickhouse_connect

log = logging.getLogger(__name__)

# Define the Connection IDs you configured in the Airflow UI
KAFKA_CONN_ID = 'kafka_local_conn'
REDIS_CONN_ID = 'redis_local_conn'
POSTGRES_CONN_ID = 'postgres_metadata_conn'
CLICKHOUSE_CONN_ID = 'clickhouse_external_conn'

# Define Kafka Topics
KAFKA_TOPIC_RAW = 'shared_raw_events'
KAFKA_TOPIC_DLQ = 'shared_dead_letter'

# Configuration for DLQ processing
DLQ_CONSUMER_GROUP_ID = 'airflow_dlq_reprocessor' # Unique consumer group for this DAG
DLQ_CONSUME_BATCH_SIZE = 100 # Number of messages to pull at once
DLQ_CONSUME_TIMEOUT_MS = 5000 # Timeout for polling Kafka
MAX_RETRY_ATTEMPTS = 3 # Max times to retry processing a message
# You might need a way to track retry counts, e.g., in Redis or a dedicated table

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True, # Enable email on failure
    'email_on_retry': False,
    'retries': 1, # Retries for the task itself, retry logic per message is handled within the task
    'retry_delay': timedelta(minutes=5),
    # 'execution_timeout': timedelta(minutes=15), # Timeout for the task run
    # 'on_failure_callback': some_alerting_function,
}

# --- Helper functions to get connections (can be shared if extracted) ---
# These are similar to the ones in the router and provisioning DAGs
def get_kafka_producer_from_conn(conn_id: str) -> KafkaProducer:
    """Gets a KafkaProducer instance from an Airflow Connection ID."""
    try:
        conn = BaseHook.get_connection(conn_id)
        brokers = conn.extra_dejson.get('brokers') # Assuming brokers are stored in extra as JSON {"brokers": "host1:port1,host2:port2"}
        if not brokers:
             # Fallback to host/port if brokers not in extra (less common for Kafka connection type)
             brokers = f"{conn.host}:{conn.port}" if conn.host else None

        if not brokers:
            raise ValueError(f"Kafka brokers not found in connection {conn_id}")

        # KafkaProducer expects a list of brokers
        broker_list = brokers.split(',') if isinstance(brokers, str) else brokers

        producer = KafkaProducer(
            bootstrap_servers=broker_list,
            value_serializer=lambda x: json.dumps(x).encode('utf-8') # Serialize outgoing messages to JSON bytes
            # Add security config (SASL/SSL) here from conn.extra if needed
            # security_protocol=conn.extra_dejson.get('security_protocol'),
            # ssl_context=...
            # sasl_plain_username=...
            # sasl_plain_password=...
        )
        # Test connection by getting cluster metadata (can take a moment)
        # producer.partitions_for_topic(KAFKA_TOPIC_RAW) # Test sending to raw topic? Or just initialize?
        log.info(f"Successfully created KafkaProducer for brokers: {broker_list}")
        return producer
    except Exception as e:
        log.error(f"Error creating KafkaProducer from connection {conn_id}: {e}", exc_info=True)
        raise

def get_kafka_consumer_from_conn(conn_id: str, group_id: str, topics: list[str]) -> KafkaConsumer:
    """Gets a KafkaConsumer instance from an Airflow Connection ID."""
    try:
        conn = BaseHook.get_connection(conn_id)
        brokers = conn.extra_dejson.get('brokers') # Assuming brokers are stored in extra
        if not brokers:
             brokers = f"{conn.host}:{conn.port}" if conn.host else None

        if not brokers:
            raise ValueError(f"Kafka brokers not found in connection {conn_id}")

        broker_list = brokers.split(',') if isinstance(brokers, str) else brokers

        consumer = KafkaConsumer(
            *topics, # Subscribe to the specified topics
            bootstrap_servers=broker_list,
            group_id=group_id, # Use the specified consumer group ID
            auto_offset_reset='earliest', # Start from earliest if no consumer group offset
            enable_auto_commit=False, # Start with manual commits, can be changed later
            auto_commit_interval_ms=5000, # Only used if auto commit is enabled
            # Add security config (SASL/SSL) here from conn.extra if needed
        )
        log.info(f"Successfully created KafkaConsumer for brokers: {broker_list}, group: {group_id}, topics: {topics}")
        # The consumer subscribes on initialization, but fetching data tests the connection
        return consumer
    except Exception as e:
        log.error(f"Error creating KafkaConsumer from connection {conn_id}: {e}", exc_info=True)
        raise

# Replicate validate_client and insert_into_clickhouse logic from the router
# Ideally, extract these into a shared module. For now, replicate the logic.

def validate_client(client_id: str, api_key_hash: str, redis_hook: RedisHook, pg_hook: PostgresHook) -> dict | None:
    """
    Validates client using Redis cache, falls back to PostgreSQL.
    Returns client metadata (client_id, api_key_hash, ch_database_name, status)
    or None if invalid/inactive or error. Requires Airflow Hooks.
    """
    cache_key = f"client:{client_id}"
    redis_client = None # Get Redis client from hook

    client_info = None

    # 1. Check Redis Cache
    try:
        redis_client = redis_hook.get_conn()
        cached_data = redis_client.get(cache_key)
        if cached_data:
            try:
                client_info = json.loads(cached_data)
                log.debug(f"Cache hit for client_id: {client_id}")
                if client_info.get("api_key_hash") == api_key_hash and client_info.get("status") == "active":
                     log.debug(f"Client valid from cache: {client_id}")
                     return client_info # Cache hit and valid
                else:
                     log.warning(f"Cache hit but client invalid/inactive for client_id: {client_id}. Invalidate cache entry.")
                     try: redis_client.delete(cache_key)
                     except Exception as e_del: log.error(f"Failed to delete cache key {cache_key}: {e_del}", exc_info=True)
            except (json.JSONDecodeError, TypeError) as e:
                log.error(f"Failed to decode/process cached data for client_id: {client_id}: {e}. Invalidating cache.", exc_info=True)
                try: redis_client.delete(cache_key)
                except Exception as e_del: log.error(f"Failed to delete cache key {cache_key} after decode error: {e_del}", exc_info=True)
            except Exception as e:
                 log.error(f"Unexpected error processing cached data for client_id: {client_id}: {e}", exc_info=True)
                 try: redis_client.delete(cache_key)
                 except Exception as e_del: log.error(f"Failed to delete cache key {cache_key} after unexpected error: {e_del}", exc_info=True)
    except Exception as e:
        log.error(f"Redis error while checking cache for {client_id}: {e}", exc_info=True)


    log.debug(f"Cache miss or invalid for client_id: {client_id}. Falling back to DB.")

    # 2. Fallback to PostgreSQL
    try:
        # Get a connection from the hook's pool
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                query = "SELECT client_id, api_key_hash, ch_database_name, status FROM clients WHERE client_id = %s AND api_key_hash = %s AND status = 'active';"
                cursor.execute(query, (client_id, api_key_hash))
                client_record = cursor.fetchone()

            if client_record:
                client_info = {
                    "client_id": str(client_record[0]),
                    "api_key_hash": client_record[1],
                    "ch_database_name": client_record[2],
                    "status": client_record[3]
                }
                log.info(f"Client validated from DB: {client_id}")
                # 3. Cache the result in Redis (best effort)
                if redis_client:
                    try:
                        redis_client.setex(cache_key, Variable.get("client_cache_ttl_seconds", default=600, deserialize_json=False), json.dumps(client_info)) # Use Airflow Variable for TTL
                        log.debug(f"Client info cached for client_id: {client_id}")
                    except Exception as e:
                         log.error(f"Failed to cache client info for {client_id}: {e}", exc_info=True)
                return client_info
            else:
                log.warning(f"Client not found, not active, or invalid hash in DB for client_id: {client_id}")
                return None

    except Exception as e:
        log.error(f"Database error while validating client {client_id}: {e}", exc_info=True)
        raise # Treat DB error as critical for validation in this context


def insert_into_clickhouse(event_data: dict, client_info: dict, ch_conn_id: str) -> bool:
    """
    Inserts event data into the correct client database in ClickHouse using clickhouse-connect.
    Returns True on success, False on failure. Requires ClickHouse Airflow Connection ID.
    
    Parameters:
    - event_data: The event data to insert
    - client_info: Client information including database name
    - ch_conn_id: ClickHouse connection ID
    """
    db_name = client_info["ch_database_name"]
    log.info(f"Attempting to insert event into ClickHouse DB: {db_name}")

    ch_client = None
    try:
        # Get ClickHouse client from Airflow Connection
        ch_client = get_ch_client_from_conn(ch_conn_id) # Use the helper function

        event_id = event_data.get('event_id')
        if event_id is None:
             event_id = str(uuid.uuid4())
             log.warning(f"Event missing event_id, generating UUID: {event_id}")

        event_time = event_data.get('event_time')
        if event_time is None:
             event_time = int(time.time()) # Use Unix timestamp in seconds
             log.warning(f"Event missing event_time, using current timestamp: {event_time}")

        event_properties_dict = event_data.get('event_properties', {})

        # Prepare data for insertion into String column
        # Make sure the column names match exactly what's in the events_base table
        data_to_insert = [{
             'event_id': event_id,
             'event_time': event_time,
             'event_properties': json.dumps(event_properties_dict) # JSON string for String column
        }]

        table_name = 'events_base'

        # clickhouse-connect insert method with explicit column_names
        ch_client.insert(
            table=table_name,
            data=data_to_insert,
            database=db_name, # Specify the target database
            column_names=['event_id', 'event_time', 'event_properties']  # Ensure these match the table columns
        )

        log.info(f"Successfully inserted 1 event into {db_name}.{table_name}")
        return True

    except Exception as e:
        log.error(f"Failed to insert event into ClickHouse DB {db_name}: {e}", exc_info=True)
        return False
    finally:
        if ch_client:
            try: ch_client.close()
            except Exception: pass


# --- Helper function to get ClickHouse client from Airflow Connection (Replicated) ---
# This needs to be available to the insert_into_clickhouse helper
def get_ch_client_from_conn(conn_id: str):
    """Gets a clickhouse-connect client from an Airflow Connection ID."""
    try:
        conn = BaseHook.get_connection(conn_id)
        
        ch_params = {
            'host': conn.host,
            'port': conn.port or 9000, # Default to 9000 if port is None in conn
            'username': conn.login,
            'password': conn.password,
        }
        
        if conn.extra:
            try:
                extra_params = conn.extra_dejson
                if 'secure' in extra_params:
                    ch_params['secure'] = extra_params['secure']
                # Add other clickhouse-connect parameters from extra if needed
            except Exception as e:
                log.warning(f"Could not parse extra parameters from ClickHouse connection: {e}")

        log.info(f"Connecting to ClickHouse with parameters (excluding password): Host={ch_params.get('host')}, Port={ch_params.get('port')}, User={ch_params.get('username')}, Secure={ch_params.get('secure')}")
        client = clickhouse_connect.get_client(**ch_params)
        log.info("Successfully created clickhouse-connect client.") # Changed log message
        return client

    except Exception as e:
        log.error(f"Error getting ClickHouse connection details or client: {e}", exc_info=True)
        raise


# --- DLQ Reprocessing Task ---
@task
def process_dlq_messages(**context):
    """
    Consumes messages from the DLQ, attempts to re-process them,
    and decides on the next action (re-publish or error).
    """
    log.info(f"Starting DLQ reprocessing task.")

    consumer = None
    producer = None

    # Get Airflow Hooks for services used in reprocessing logic
    redis_hook = RedisHook(redis_conn_id=REDIS_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    try:
        # 1. Get Kafka Consumer for DLQ
        consumer = get_kafka_consumer_from_conn(KAFKA_CONN_ID, DLQ_CONSUMER_GROUP_ID, [KAFKA_TOPIC_DLQ])
        log.info(f"Kafka Consumer subscribed to DLQ topic: {KAFKA_TOPIC_DLQ}")

        # Assign to partitions if you want to ensure specific partition processing (optional)
        # consumer.assign([TopicPartition(KAFKA_TOPIC_DLQ, i) for i in range(consumer.partitions_for_topic(KAFKA_TOPIC_DLQ))])

        # Seek to beginning if needed (e.g., on first run)
        # consumer.seek_to_beginning()
        # log.info("Seeked to beginning of DLQ topic.")


        # 2. Get Kafka Producer (for republishing to raw topic)
        producer = get_kafka_producer_from_conn(KAFKA_CONN_ID)
        log.info("Kafka Producer created.")

        # 3. Poll for messages from the DLQ
        # poll returns {TopicPartition: [messages]}
        messages_by_partition = consumer.poll(
            max_records=DLQ_CONSUME_BATCH_SIZE,
            timeout_ms=DLQ_CONSUME_TIMEOUT_MS
        )

        if not messages_by_partition:
            log.info("No messages found in the DLQ.")
            return # Exit task successfully if no messages

        total_processed = 0
        republished_count = 0
        permanently_failed_count = 0
        processed_offsets = {} # To track offsets for committing

        # Process messages batch by batch
        for partition, messages in messages_by_partition.items():
            log.info(f"Processing {len(messages)} messages from partition {partition}.")
            for message in messages:
                log.info(f"Processing DLQ message from offset {message.offset}")
                original_payload_bytes = message.value # The value from the DLQ message is our original payload

                try:
                    # The DLQ message value is the JSON object we put there
                    dlq_message_content = json.loads(original_payload_bytes.decode('utf-8'))
                    
                    # Extract original payload and reason
                    original_event_payload_raw = dlq_message_content.get('original_payload_raw')
                    failure_reason = dlq_message_content.get('reason')
                    # Other info like timestamp_utc can be logged

                    if not original_event_payload_raw:
                        log.warning(f"DLQ message at offset {message.offset} missing original_payload_raw. Cannot reprocess.")
                        permanently_failed_count += 1
                        # Decide how to handle unprocessable DLQ messages (e.g., move to separate error topic, alert)
                        continue # Move to next message

                    try:
                        # Try to decode the original event payload
                        original_event_payload_dict = json.loads(original_event_payload_raw)
                        log.info(f"Attempting to reprocess original event: {original_event_payload_dict}. Reason: {failure_reason}")

                        # --- Re-processing Logic ---
                        # This is similar to the Router's process_event logic
                        # We need client_id and api_key_hash to validate

                        client_id = original_event_payload_dict.get('client_id')
                        # Assuming api_key_hash is in the original event payload
                        api_key_hash = original_event_payload_dict.get('api_key_hash')

                        if not client_id or not api_key_hash:
                            log.warning(f"Original event missing client_id or api_key_hash. Cannot reprocess. Offset: {message.offset}")
                            permanently_failed_count += 1
                            # Decide how to handle events still missing key info
                            continue

                        # Re-validate the client using Redis/PG Hooks
                        client_info = validate_client(client_id, api_key_hash, redis_hook, pg_hook)

                        if client_info and client_info.get('status') == 'active':
                            # Client is now valid and active, attempt insertion again
                            log.info(f"Client {client_id} is valid. Attempting to re-insert into ClickHouse.")
                            
                            # Use the insert_into_clickhouse helper with correct parameter order
                            insert_success = insert_into_clickhouse(
                                event_data=original_event_payload_dict, 
                                client_info=client_info, 
                                ch_conn_id=CLICKHOUSE_CONN_ID
                            )

                            if insert_success:
                                log.info(f"Successfully re-inserted message from DLQ offset {message.offset} for client {client_id}.")
                                republished_count += 1 # Count as successful reprocessing

                            else:
                                log.warning(f"Re-insertion to ClickHouse failed for DLQ offset {message.offset}. Client {client_id}. Reason: {failure_reason}")
                                # Decide on retry policy. Could increment a retry counter in Redis/PG.
                                # If max retries reached, move to permanent error storage.
                                # For now, if re-insert fails, count as permanently failed for this run.
                                permanently_failed_count += 1

                        else:
                            log.warning(f"Client validation failed during reprocessing for DLQ offset {message.offset}. Client {client_id}. Reason: {failure_reason}")
                            permanently_failed_count += 1
                            # Client is still invalid or inactive. Move to permanent error storage or alert.


                    except json.JSONDecodeError:
                         log.error(f"Failed to decode original event payload (raw): {original_event_payload_raw}. Offset: {message.offset}. Cannot reprocess.", exc_info=True)
                         permanently_failed_count += 1
                         # Original payload was not valid JSON, cannot reprocess

                    except Exception as e:
                        log.error(f"An unexpected error occurred during reprocessing for DLQ offset {message.offset}: {e}", exc_info=True)
                        permanently_failed_count += 1 # Count as permanently failed for this run
                        # Decide if this error warrants retry later vs permanent failure

                except json.JSONDecodeError:
                    log.error(f"Failed to decode DLQ message value (raw): {original_payload_bytes.decode('utf-8', errors='ignore')}. Offset: {message.offset}. Cannot process DLQ message.", exc_info=True)
                    permanently_failed_count += 1
                    # The DLQ message format itself is wrong.

                except Exception as e:
                    log.error(f"An unexpected error occurred processing DLQ message at offset {message.offset}: {e}", exc_info=True)
                    permanently_failed_count += 1 # Count as permanently failed for this run


                total_processed += 1
                # Track the highest offset processed for this partition for committing
                processed_offsets[partition] = message.offset

        log.info(f"Finished processing batch. Total processed: {total_processed}, Republished: {republished_count}, Permanently Failed (this run): {permanently_failed_count}")


        # 4. Commit offsets for processed messages
        # This commits the HIGHEST offset processed for each partition in this batch.
        # Messages up to this offset will not be re-delivered to this consumer group.
        # Only commit if you are confident that messages up to this offset were handled
        # (either successfully reprocessed or moved to permanent failure storage).
        if processed_offsets:
            log.info(f"Processed message offsets: {processed_offsets}")
            
            # Simplify offset commit - use auto commit instead of manual commit
            try:
                # Enable auto commit temporarily
                consumer._enable_auto_commit = True
                consumer._auto_commit_interval_ms = 100
                
                # Poll with zero timeout to trigger auto commit
                consumer.poll(0)
                
                # Small sleep to allow auto commit to complete
                time.sleep(0.5)
                
                log.info("Auto commit triggered for processed offsets")
            except Exception as e:
                log.error(f"Error during offset commit: {e}", exc_info=True)
                # Don't fail the task on commit error


    except Exception as e:
        # Catch any errors during consumer/producer creation or batch processing
        log.error(f"An error occurred during DLQ reprocessing task: {e}", exc_info=True)
        # If this is a transient error (e.g., Kafka connection), task retry might help.
        raise # Re-raise to fail the task

    finally:
        # Close Kafka consumer and producer connections
        if consumer:
            try:
                 # Commit final offsets before closing (optional, done above per batch)
                 # consumer.commit_sync()
                 consumer.close()
                 log.info("Kafka consumer closed.")
            except Exception as e:
                log.error(f"Error closing Kafka consumer: {e}")
        if producer:
            try:
                producer.flush() # Ensure all buffered messages are sent
                producer.close()
                log.info("Kafka producer closed.")
            except Exception as e:
                log.error(f"Error closing Kafka producer: {e}")
        # Database and Redis connections are managed by Hooks and closed automatically


# --- Define the DAG using TaskFlow API ---
@dag(
    dag_id='dlq_reprocessing_dag',
    default_args=default_args,
    description='Reprocesses messages from the Kafka Dead Letter Queue.',
    schedule=None,  # Set to None to disable automatic scheduling
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['realtime', 'dlq', 'kafka'],
)
def dlq_reprocessing_dag():
    """DAG for reprocessing messages from the Kafka Dead Letter Queue."""
    # Create task instance
    reprocess_dlq_task = process_dlq_messages()
    
    # Define task dependencies if needed
    # upstream_task >> reprocess_dlq_task >> downstream_task
    
    # Return task or nothing
    return reprocess_dlq_task

# Instantiate the DAG
dlq_reprocessing_dag_instance = dlq_reprocessing_dag()