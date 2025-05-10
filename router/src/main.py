# your-analytics-monorepo/router/src/main.py
import os
import logging
import json
import time
import uuid
import hashlib
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

# Import actual client libraries
from kafka import KafkaConsumer, KafkaProducer
import redis
import psycopg2.pool
# --- CHANGE: Import clickhouse_connect ---
import clickhouse_connect
# --- END CHANGE ---

# --- Configuration Loading ---
# ... (Kafka, Redis, PG config - same as before) ...

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
KAFKA_TOPIC_RAW = os.getenv('KAFKA_TOPIC_RAW', 'shared_raw_events')
KAFKA_TOPIC_DLQ = os.getenv('KAFKA_TOPIC_DLQ', 'shared_dead_letter')
KAFKA_CONSUMER_GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP_ID', 'analytics_router_group')

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
CLIENT_CACHE_TTL_SECONDS = int(os.getenv('CLIENT_CACHE_TTL_SECONDS', '600'))

# Configuration for EXTERNAL databases - require these environment variables
CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST')
CLICKHOUSE_PORT_HTTP = int(os.environ.get('CLICKHOUSE_PORT_HTTP', '8123'))  # HTTP protocol port
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD')

POSTGRES_METADATA_HOST = os.environ.get('POSTGRES_METADATA_HOST')
POSTGRES_METADATA_PORT = int(os.environ.get('POSTGRES_METADATA_PORT', '5432'))
POSTGRES_METADATA_DB = os.environ.get('POSTGRES_METADATA_DB')
POSTGRES_METADATA_USER = os.environ.get('POSTGRES_METADATA_USER')
POSTGRES_METADATA_PASSWORD = os.environ.get('POSTGRES_METADATA_PASSWORD')

ROUTER_LISTEN_PORT = int(os.getenv('ROUTER_LISTEN_PORT', '8080'))

required_vars = {
    'CLICKHOUSE_HOST': CLICKHOUSE_HOST,
    'CLICKHOUSE_USER': CLICKHOUSE_USER,
    'CLICKHOUSE_PASSWORD': CLICKHOUSE_PASSWORD,
    'POSTGRES_METADATA_HOST': POSTGRES_METADATA_HOST,
    'POSTGRES_METADATA_DB': POSTGRES_METADATA_DB,
    'POSTGRES_METADATA_USER': POSTGRES_METADATA_USER,
    'POSTGRES_METADATA_PASSWORD': POSTGRES_METADATA_PASSWORD,
    'REDIS_HOST': REDIS_HOST,
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    logging.critical(f"Missing required environment variables: {', '.join(missing_vars)}")
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Service Clients ---
# Initialize global variables for clients/pools
kafka_consumer: KafkaConsumer | None = None
kafka_producer: KafkaProducer | None = None
redis_client: redis.StrictRedis | None = None
pg_conn_pool: psycopg2.pool.SimpleConnectionPool | None = None
# --- CHANGE: Fix clickhouse_connect client type ---
ch_client: Any | None = None  # Using Any since clickhouse_connect doesn't expose its client type
# --- END CHANGE ---


def connect_to_services():
    """Initializes connections/pools to Kafka, Redis, PG, ClickHouse."""
    logger.info("Applying startup delay...")
    time.sleep(35)
    logger.info("Startup delay finished. Attempting connections.")

    global kafka_consumer, kafka_producer, redis_client, pg_conn_pool, ch_client

    # Kafka Connection
    logger.info(f"Connecting to Kafka brokers: {KAFKA_BROKERS}")
    try:
        kafka_consumer = KafkaConsumer(
            KAFKA_TOPIC_RAW,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=KAFKA_CONSUMER_GROUP_ID,
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        kafka_consumer.topics() # Test connection
        logger.info("Connected to Kafka.")
    except Exception as e:
        logger.critical(f"Failed to connect to Kafka: {e}", exc_info=True)
        raise

    # Redis Connection
    logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
    try:
        redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        redis_client.ping() # Test connection
        logger.info("Connected to Redis.")
    except Exception as e:
        logger.critical(f"Failed to connect to Redis: {e}", exc_info=True)
        raise

    # PostgreSQL Connection Pool
    logger.info(f"Connecting to PostgreSQL metadata DB at {POSTGRES_METADATA_HOST}:{POSTGRES_METADATA_PORT}")
    try:
        pg_conn_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10,
            host=POSTGRES_METADATA_HOST,
            port=POSTGRES_METADATA_PORT,
            database=POSTGRES_METADATA_DB,
            user=POSTGRES_METADATA_USER,
            password=POSTGRES_METADATA_PASSWORD
        )
        with pg_conn_pool.getconn() as conn:
           with conn.cursor() as cur:
               cur.execute("SELECT 1")
        logger.info("Connected to PostgreSQL and pool initialized.")
    except Exception as e:
        logger.critical(f"Failed to connect to PostgreSQL: {e}", exc_info=True)
        raise

    # ClickHouse Connection (using clickhouse-connect)
    logger.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT_HTTP}")
    try:
        ch_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT_HTTP,  # Use HTTP port
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database='default'
        )
        ch_client.command('SELECT 1')  # Test connection
        logger.info("Connected to ClickHouse.")
    except Exception as e:
        logger.critical(f"Failed to connect to ClickHouse: {e}", exc_info=True)
        raise


def validate_client(client_id: str, api_key_hash: str) -> dict | None:
    # ... (This function does NOT use the ClickHouse client directly, so no changes needed here) ...
    """
    Validates client using Redis cache, falls back to PostgreSQL.
    Returns client metadata (client_id, api_key_hash, ch_database_name, status)
    or None if invalid/inactive or error.
    """
    cache_key = f"client:{client_id}"
    client_info = None

    if redis_client:
        try:
            cached_data = redis_client.get(cache_key)
            if cached_data:
                try:
                    client_info = json.loads(cached_data)
                    logger.debug(f"Cache hit for client_id: {client_id}")
                    if client_info.get("api_key_hash") == api_key_hash and client_info.get("status") == "active":
                        logger.debug(f"Client valid from cache: {client_id}")
                        return client_info
                    else:
                         logger.warning(f"Cache hit but client invalid/inactive for client_id: {client_id}. Invalidate cache entry.")
                         try:
                             redis_client.delete(cache_key)
                         except Exception as e_del:
                             logger.error(f"Failed to delete cache key {cache_key}: {e_del}", exc_info=True)

                except (json.JSONDecodeError, TypeError) as e:
                    logger.error(f"Failed to decode/process cached data for client_id: {client_id}: {e}. Invalidating cache.", exc_info=True)
                    try:
                        redis_client.delete(cache_key)
                    except Exception as e_del:
                        logger.error(f"Failed to delete cache key {cache_key} after decode error: {e_del}", exc_info=True)
                except Exception as e:
                     logger.error(f"Unexpected error processing cached data for client_id: {client_id}: {e}", exc_info=True)
                     try:
                        redis_client.delete(cache_key)
                     except Exception as e_del:
                        logger.error(f"Failed to delete cache key {cache_key} after unexpected error: {e_del}", exc_info=True)

        except Exception as e:
            logger.error(f"Redis error while checking cache for {client_id}: {e}", exc_info=True)

    logger.debug(f"Cache miss or invalid for client_id: {client_id}. Falling back to DB.")

    conn = None
    cursor = None
    if pg_conn_pool:
        try:
            conn = pg_conn_pool.getconn()
            cursor = conn.cursor()
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
                logger.info(f"Client validated from DB: {client_id}")
                if redis_client:
                    try:
                        redis_client.setex(cache_key, CLIENT_CACHE_TTL_SECONDS, json.dumps(client_info))
                        logger.debug(f"Client info cached for client_id: {client_id}")
                    except Exception as e:
                        logger.error(f"Failed to cache client info for {client_id}: {e}", exc_info=True)
                return client_info
            else:
                logger.warning(f"Client not found, not active, or invalid hash in DB for client_id: {client_id}")
                return None

        except Exception as e:
            logger.error(f"Database error while validating client {client_id}: {e}", exc_info=True)
            return None
        finally:
            if cursor:
                try: cursor.close()
                except Exception: pass
            if conn:
                try: pg_conn_pool.putconn(conn)
                except Exception: pass

    else:
        logger.error(f"PostgreSQL connection pool is not initialized. Cannot validate client {client_id}.")
        return None


def insert_into_clickhouse(client_info: dict, event_data: dict) -> bool:
    """
    Inserts event data into the correct client database in ClickHouse using clickhouse-connect.
    Returns True on success, False on failure.
    """
    db_name = client_info["ch_database_name"]

    if ch_client is None:
        logger.error("ClickHouse client is not initialized. Cannot insert event.")
        return False

    try:
        event_id = event_data.get('event_id')
        if event_id is None:
             event_id = str(uuid.uuid4())
             logger.warning(f"Event missing event_id, generating UUID: {event_id}")

        event_time = event_data.get('event_time')
        if event_time is None:
             event_time = int(time.time()) # Use Unix timestamp in seconds
             logger.warning(f"Event missing event_time, using current timestamp: {event_time}")

        event_properties_dict = event_data.get('event_properties', {})

        # --- CHANGE: Prepare data and use clickhouse-connect client.insert ---
        # clickhouse-connect.insert expects the table name, a list of data rows,
        # and optional database name. It handles serialization based on column types.
        # If event_properties is String, we pass a JSON string.
        # If event_properties were JSON (and driver/server compatible), we could pass the dict.

        # Assuming the ClickHouse column is now String, we need to pass a JSON string
        data_row = [
            event_id, # UUID
            event_time, # DateTime (int seconds)
            json.dumps(event_properties_dict) # JSON string for String column
            # Add other top-level fields here
        ]
        data_to_insert = [data_row] # insert method expects a list of rows

        table_name = 'events_base'

        # clickhouse-connect.insert method
        # It automatically determines column types from the server and serializes data
        ch_client.insert(
            table=table_name,
            data=data_to_insert,
            database=db_name # Specify the target database
            # column_names=['event_id', 'event_time', 'event_properties'] # Optional but good practice
        )

        logger.info(f"Successfully inserted 1 event into {db_name}.{table_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to insert event into ClickHouse DB {db_name}: {e}", exc_info=True)
        # Log specific clickhouse-connect errors if needed
        # import clickhouse_connect.driver.exceptions as ch_exc
        # if isinstance(e, ch_exc.ClickHouseError):
        #      logger.error(f"ClickHouse error details: Code {e.code}, Text: {e.message}")
        return False
    # --- END CHANGE ---


# ... (rest of process_event, produce_to_dlq, run_router, HealthCheckHandler, etc. - should not need changes) ...

def process_event(kafka_message) -> bool:
    """
    Processes a single event message from Kafka.
    Returns True if message was successfully processed (validated and inserted), False otherwise.
    """
    original_payload_bytes = kafka_message.value

    try:
        event_data = json.loads(original_payload_bytes.decode('utf-8'))
        logger.info(f"Received event: {event_data}")

        client_id = event_data.get('client_id')
        api_key_hash = event_data.get('api_key_hash')

        if not client_id or not api_key_hash:
            logger.warning(f"Event missing client_id or api_key_hash. Sending to DLQ. Event: {event_data}")
            produce_to_dlq(original_payload_bytes, "missing_client_id_or_hash")
            return False

        client_info = validate_client(client_id, api_key_hash)

        if client_info and client_info.get('status') == 'active':
            success = insert_into_clickhouse(client_info, event_data)
            if not success:
                produce_to_dlq(original_payload_bytes, "clickhouse_insert_failed")
            return success
        else:
            logger.warning(f"Client validation failed for client_id: {client_id}. Sending to DLQ. Event: {event_data}")
            produce_to_dlq(original_payload_bytes, "client_validation_failed")
            return False

    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from Kafka message: {original_payload_bytes}. Sending to DLQ.", exc_info=True)
        produce_to_dlq(original_payload_bytes, "json_decode_error")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred processing message {kafka_message.offset}: {e}. Sending to DLQ.", exc_info=True)
        produce_to_dlq(original_payload_bytes, f"unexpected_error: {type(e).__name__}")
        return False

def produce_to_dlq(original_payload: bytes, reason: str):
    """Produces a message to the Dead Letter Queue topic."""
    if kafka_producer is None:
        logger.critical("Kafka producer is not initialized, cannot send to DLQ!")
        logger.error(f"Original message lost: {original_payload.decode('utf-8', errors='ignore')}")
        return

    try:
        dlq_message = {
            "original_payload_raw": original_payload.decode('utf-8', errors='ignore'),
            "reason": reason,
            "timestamp_utc": int(time.time())
        }
        future = kafka_producer.send(KAFKA_TOPIC_DLQ, value=dlq_message)
        future.get(timeout=10)
        logger.info(f"Sent message to DLQ ({KAFKA_TOPIC_DLQ}) due to: {reason}")
    except Exception as e:
        logger.error(f"Failed to send message to DLQ: {e}", exc_info=True)
        logger.critical(f"Message failed to go to DLQ and is potentially lost. Reason: {reason}. Original payload: {original_payload.decode('utf-8', errors='ignore')}")

def run_router():
    """Main function to connect to services and start consuming Kafka messages."""
    logger.info("Starting Analytics Router...")
    connect_to_services()

    logger.info(f"Listening for messages on Kafka topic: {KAFKA_TOPIC_RAW}")

    try:
        for message in kafka_consumer:
            logger.info(f"Processing message from topic {message.topic}, partition {message.partition}, offset {message.offset}")

            processed_successfully = process_event(message)

            if processed_successfully:
                 logger.info(f"Message {message.offset} processed successfully.")
                 try:
                     kafka_consumer.commit()
                     logger.debug(f"Committed offset {message.offset} for partition {message.partition}")
                 except Exception as e:
                     logger.error(f"Failed to commit offset {message.offset}: {e}", exc_info=True)

            else:
                 logger.warning(f"Message {message.offset} failed processing. Sent to DLQ.")
                 pass

    except KeyboardInterrupt:
        logger.info("Shutdown signal received (KeyboardInterrupt). Shutting down router.")
        pass
    except Exception as e:
         logger.critical(f"Kafka consumption loop failed unexpectedly: {e}", exc_info=True)
         raise

    finally:
        logger.info("Router consumption loop finished. Cleaning up connections.")
        if kafka_consumer:
            try:
                kafka_consumer.commit_sync()
                kafka_consumer.close()
                logger.info("Kafka consumer closed.")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
        if kafka_producer:
            try:
                kafka_producer.flush()
                kafka_producer.close()
                logger.info("Kafka producer closed.")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
        if redis_client:
            try:
                redis_client.close()
                logger.info("Redis client closed.")
            except Exception as e:
                logger.error(f"Error closing Redis client: {e}")
        if pg_conn_pool:
            try:
                pg_conn_pool.closeall()
                logger.info("PostgreSQL connection pool closed.")
            except Exception as e:
                logger.error(f"Error closing PG pool: {e}")
        if ch_client:
            try:
                 # clickhouse-connect client has a close method
                 ch_client.close()
                 logger.info("ClickHouse client closed.")
            except Exception as e:
                logger.error(f"Error closing ClickHouse client: {e}")

# --- Health Check Server ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/healthz':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass

health_check_server: HTTPServer | None = None

def start_health_check_server():
    global health_check_server
    try:
        server_address = ('0.0.0.0', ROUTER_LISTEN_PORT)
        health_check_server = HTTPServer(server_address, HealthCheckHandler)
        logger.info(f"Health check server listening on port {ROUTER_LISTEN_PORT}")
        health_check_server.serve_forever()
    except Exception as e:
        logger.error(f"Failed to start health check server: {e}", exc_info=True)


def stop_health_check_server():
    global health_check_server
    if health_check_server:
        logger.info("Shutting down health check server.")
        try:
            health_check_server.shutdown()
            health_check_server.server_close()
        except Exception as e:
            logger.error(f"Error shutting down health check server: {e}")


if __name__ == "__main__":
    health_thread = threading.Thread(target=start_health_check_server)
    health_thread.daemon = True
    health_thread.start()
    logger.info("Health check thread started.")

    try:
        run_router()
    except Exception as e:
        logger.critical(f"Router initialization or main loop failed: {e}", exc_info=True)
        os._exit(1)

    finally:
        logger.info("Main router process finished.")
        stop_health_check_server()
        logger.info("Router process exiting.")