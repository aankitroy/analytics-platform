# your-analytics-monorepo/router/src/main.py
import os
import logging
import json
import time
from kafka import KafkaConsumer, KafkaProducer # Assuming you'll use kafka-python
# You'll need libraries for Redis, PostgreSQL, and ClickHouse based on your Go/Python choice
# Example imports for Python:
# import redis
# import psycopg2 # Or a different PG adapter like pg8000
# from clickhouse_driver import Client as ClickHouseClient

# Load environment variables from .env file if it exists

# --- Configuration Loading ---
# Load configuration from environment variables
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
KAFKA_TOPIC_RAW = os.getenv('KAFKA_TOPIC_RAW', 'shared_raw_events')
KAFKA_TOPIC_DLQ = os.getenv('KAFKA_TOPIC_DLQ', 'shared_dead_letter')
KAFKA_CONSUMER_GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP_ID', 'analytics_router_group')

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
CLIENT_CACHE_TTL_SECONDS = int(os.getenv('CLIENT_CACHE_TTL_SECONDS', '600'))

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT_NATIVE = int(os.getenv('CLICKHOUSE_PORT_NATIVE', '9000'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')

POSTGRES_METADATA_HOST = os.getenv('POSTGRES_METADATA_HOST', 'localhost')
POSTGRES_METADATA_PORT = int(os.getenv('POSTGRES_METADATA_PORT', '5432'))
POSTGRES_METADATA_DB = os.getenv('POSTGRES_METADATA_DB', 'analytics_metadata')
POSTGRES_METADATA_USER = os.getenv('POSTGRES_METADATA_USER', 'aankitroy')
POSTGRES_METADATA_PASSWORD = os.getenv('POSTGRES_METADATA_PASSWORD', '')

# Router Configuration
ROUTER_LISTEN_PORT = int(os.getenv('ROUTER_LISTEN_PORT', '8080'))

# Validate required environment variables
required_vars = {
    'CLICKHOUSE_PASSWORD': CLICKHOUSE_PASSWORD,
    'POSTGRES_METADATA_PASSWORD': POSTGRES_METADATA_PASSWORD
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Service Connections (Placeholder) ---
# In a real implementation, set up proper connection pooling and error handling
kafka_consumer = None
kafka_producer = None
redis_client = None
pg_conn_pool = None # Use connection pooling for PG
ch_client = None # Use a ClickHouse client/pool

def connect_to_services():
    """Initializes connections to Kafka, Redis, PG, ClickHouse."""
    global kafka_consumer, kafka_producer, redis_client #, pg_conn_pool, ch_client

    logger.info(f"Connecting to Kafka brokers: {KAFKA_BROKERS}")
    try:
        kafka_consumer = KafkaConsumer(
            KAFKA_TOPIC_RAW,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=KAFKA_CONSUMER_GROUP_ID,
            auto_offset_reset='earliest', # Start consuming from the beginning if no offset found
            enable_auto_commit=False # We'll commit manually after successful processing
        )
        kafka_producer = KafkaProducer(
             bootstrap_servers=KAFKA_BROKERS,
             value_serializer=lambda x: json.dumps(x).encode('utf-8') # Serialize outgoing messages to JSON bytes
        )
        logger.info("Connected to Kafka.")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        # In a real app, you'd handle this (e.g., exit, retry)
        raise

    logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
    try:
        # redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        # redis_client.ping() # Check connection
        logger.info("Connected to Redis.")
        # Placeholder connection (replace with actual Redis client)
        class MockRedis:
            def get(self, key): return None # Simulate cache miss
            def setex(self, key, ttl, value): pass
        redis_client = MockRedis()

    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        # In a real app, decide if Redis is critical or if you can operate without cache

    logger.info(f"Connecting to PostgreSQL at {POSTGRES_METADATA_HOST}:{POSTGRES_METADATA_PORT}")
    try:
        # Example using psycopg2 (requires installation)
        # pg_conn_pool = psycopg2.pool.SimpleConnectionPool(
        #     1, 10, # min conn, max conn
        #     host=POSTGRES_METADATA_HOST,
        #     port=POSTGRES_METADATA_PORT,
        #     database=POSTGRES_METADATA_DB,
        #     user=POSTGRES_METADATA_USER,
        #     password=POSTGRES_METADATA_PASSWORD
        # )
        # with pg_conn_pool.getconn() as conn:
        #    with conn.cursor() as cur:
        #        cur.execute("SELECT 1")
        logger.info("Connected to PostgreSQL.")
         # Placeholder connection (replace with actual PG client/pool)
        class MockPgConn:
             def cursor(self): return MockPgCursor()
             def close(self): pass
             def commit(self): pass
        class MockPgCursor:
             def execute(self, query, params=None): pass
             def fetchone(self):
                 # Simulate finding the test client defined in 02_insert_test_client.sql
                 test_client_id = 'a1b2c3d4-e5f6-7890-1234-567890abcdef'
                 test_api_key_hash = 'c44b61ce039b4637f3a7cb92dcc445876a4720d25d1c9614fe08fc734f2da20f' # **MATCH THE HASH FROM SQL FILE**
                 test_db_name = 'client_test'
                 return (test_client_id, test_api_key_hash, test_db_name, 'active') # (client_id, api_key_hash, ch_database_name, status)
             def close(self): pass
        class MockPgPool:
            def getconn(self): return MockPgConn()
            def putconn(self, conn): pass
        pg_conn_pool = MockPgPool()


    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        # PG is critical as it's the source of truth for clients
        raise


    logger.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT_NATIVE}")
    try:
        # Example using clickhouse-driver (requires installation)
        # ch_client = ClickHouseClient(
        #     host=CLICKHOUSE_HOST,
        #     port=CLICKHOUSE_PORT_NATIVE, # Use the native port
        #     user=CLICKHOUSE_USER,
        #     password=CLICKHOUSE_PASSWORD,
        #     database=None # Connect without specifying a DB initially, will switch later
        # )
        # ch_client.execute("SELECT 1") # Check connection
        logger.info("Connected to ClickHouse.")
        # Placeholder connection (replace with actual CH client)
        class MockClickHouseClient:
             def execute(self, query, params=None):
                 logger.info(f"Mock ClickHouse Query: {query}, Params: {params}")
                 # Simulate successful insert
                 if "INSERT INTO" in query:
                      logger.info("Mock Insert successful")
                      return [] # Return empty list for successful insert

             def execute_batch(self, query, params):
                  logger.info(f"Mock ClickHouse Batch Query: {query}, Params: {params}")
                  # Simulate successful batch insert
                  if "INSERT INTO" in query:
                      logger.info("Mock Batch Insert successful")
                      return []

             def use(self, database_name):
                 logger.info(f"Mock ClickHouse: Using database {database_name}")
                 # Simulate switching database context
                 self.current_database = database_name

             def close(self): pass

        ch_client = MockClickHouseClient()


    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        # ClickHouse is critical for storing data
        raise


def validate_client(client_id, api_key_hash):
    """
    Validates client using Redis cache, falls back to PostgreSQL.
    Returns client metadata (db_name, status) or None if invalid/inactive.
    """
    cache_key = f"client:{client_id}"
    # logger.debug(f"Checking cache for client_id: {client_id}") # Enable for debugging cache

    # 1. Check Redis Cache (Placeholder uses MockRedis)
    # cached_data = redis_client.get(cache_key)
    # if cached_data:
    #     try:
    #         client_info = json.loads(cached_data)
    #         logger.debug(f"Cache hit for client_id: {client_id}")
    #         # Check if status is active from cache
    #         if client_info.get("status") == "active":
    #             # Also re-validate API key hash from cache for security
    #             if client_info.get("api_key_hash") == api_key_hash:
    #                  return client_info # Cache hit and valid
    #             else:
    #                  logger.warning(f"API key hash mismatch for client_id: {client_id} (Cache hit but invalid hash)")
    #                  # Invalidate cache entry on hash mismatch
    #                  # redis_client.delete(cache_key)
    #                  # Fallback to PG
    #         else:
    #              logger.debug(f"Cache hit but client not active for client_id: {client_id} (Status: {client_info.get('status')})")
    #              # Don't cache inactive clients? Or refresh cache?
    #              return None # Not active
    #     except json.JSONDecodeError:
    #         logger.error(f"Failed to decode cached data for client_id: {client_id}. Invalidating cache.")
    #         # redis_client.delete(cache_key)
    #     except Exception as e:
    #          logger.error(f"Error processing cached data for client_id: {client_id}: {e}")
    #          # redis_client.delete(cache_key)

    # logger.debug(f"Cache miss or invalid for client_id: {client_id}. Falling back to DB.")

    # 2. Fallback to PostgreSQL (Placeholder uses MockPgPool)
    conn = None
    cursor = None
    try:
        conn = pg_conn_pool.getconn()
        cursor = conn.cursor()
        # **IMPORTANT**: Never put api_key_hash directly in the query string. Use parameters!
        query = "SELECT client_id, api_key_hash, ch_database_name, status FROM clients WHERE client_id = %s AND api_key_hash = %s AND status = 'active';"
        cursor.execute(query, (client_id, api_key_hash))
        client_record = cursor.fetchone()

        if client_record:
            # (client_id, api_key_hash, ch_database_name, status)
            client_info = {
                "client_id": str(client_record[0]), # Ensure UUID is string
                "api_key_hash": client_record[1],
                "ch_database_name": client_record[2],
                "status": client_record[3]
            }
            logger.info(f"Client validated from DB: {client_id}")
            # 3. Cache the result (Placeholder uses MockRedis)
            # try:
            #     redis_client.setex(cache_key, CLIENT_CACHE_TTL_SECONDS, json.dumps(client_info))
            #     logger.debug(f"Client info cached for client_id: {client_id}")
            # except Exception as e:
            #      logger.error(f"Failed to cache client info for {client_id}: {e}")
            return client_info
        else:
            logger.warning(f"Client not found or not active in DB for client_id: {client_id}")
            return None # Not found or not active/invalid hash

    except Exception as e:
        logger.error(f"Database error while validating client {client_id}: {e}")
        # Depending on error, maybe retry or send to DLQ
        return None
    finally:
        if cursor: cursor.close()
        if conn: pg_conn_pool.putconn(conn)


def insert_into_clickhouse(client_info, event_data):
    """
    Inserts event data into the correct client database in ClickHouse.
    """
    db_name = client_info["ch_database_name"]
    # logger.debug(f"Inserting event into ClickHouse DB: {db_name}")

    # Placeholder uses MockClickHouseClient
    try:
        # In a real client library, you'd switch database context or include it in the query
        # ch_client.use(db_name)

        # Prepare data for insertion. Ensure event_properties is a JSON string if ClickHouse expects JSON
        # Example data structure: [{'event_id': '...', 'event_time': '...', 'event_properties': '{...}'}]
        # You need to adapt this based on how you want to flatten/process event_data
        # For now, let's assume event_data is the dictionary payload and we convert event_properties to JSON string
        data_to_insert = [{
             'event_id': event_data.get('event_id'), # Assuming event_id is in payload
             'event_time': event_data.get('event_time'), # Assuming event_time is in payload
             'event_properties': json.dumps(event_data.get('event_properties', {})) # Assuming properties is a dict
        }]
        # logger.debug(f"Data to insert: {data_to_insert}")

        # Example using execute_batch for better performance
        # query = f"INSERT INTO {db_name}.events_base (event_id, event_time, event_properties) VALUES"
        # ch_client.execute_batch(query, data_to_insert) # Use execute_batch for multiple rows

        # Placeholder mock execution
        ch_client.use(db_name)
        ch_client.execute("INSERT INTO {db_name}.events_base (event_id, event_time, event_properties) VALUES", data_to_insert)

        logger.info(f"Successfully inserted event into {db_name}.events_base")
        return True

    except Exception as e:
        logger.error(f"Failed to insert event into ClickHouse DB {db_name}: {e}")
        return False


def process_event(kafka_message):
    """Processes a single event message from Kafka."""
    try:
        # Deserialize event data (assuming JSON)
        # message.value is bytes
        event_data = json.loads(kafka_message.value)
        logger.info(f"Received event: {event_data}")

        # Extract client identification (assuming it's in the event payload)
        client_id = event_data.get('client_id')
        api_key = event_data.get('api_key') # Or raw key to hash, or hash directly
        # **IMPORTANT**: If receiving raw key, hash it *before* validation!
        # For this placeholder, let's assume the hash is already in the payload for simplicity
        api_key_hash = event_data.get('api_key_hash')


        if not client_id or not api_key_hash:
            logger.warning(f"Event missing client_id or api_key_hash. Sending to DLQ. Event: {event_data}")
            produce_to_dlq(kafka_message.value, "missing_client_id_or_hash")
            return False # Indicate processing failure

        # Validate the client
        client_info = validate_client(client_id, api_key_hash)

        if client_info and client_info.get('status') == 'active':
            # Client is valid and active, proceed with insertion
            success = insert_into_clickhouse(client_info, event_data)
            if not success:
                 # If insertion failed, send to DLQ
                 produce_to_dlq(kafka_message.value, "clickhouse_insert_failed")
            return success # Indicate processing success or failure
        else:
            # Client validation failed (not found, invalid hash, or inactive)
            logger.warning(f"Client validation failed for client_id: {client_id}. Sending to DLQ. Event: {event_data}")
            produce_to_dlq(kafka_message.value, "client_validation_failed")
            return False # Indicate processing failure

    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from Kafka message: {kafka_message.value}. Sending to DLQ.")
        produce_to_dlq(kafka_message.value, "json_decode_error")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred processing message {kafka_message.offset}: {e}. Sending to DLQ.")
        produce_to_dlq(kafka_message.value, f"unexpected_error: {type(e).__name__}")
        return False

def produce_to_dlq(original_payload, reason):
    """Produces a message to the Dead Letter Queue topic."""
    try:
        dlq_message = {
            "original_payload": json.loads(original_payload.decode('utf-8')), # Decode bytes to string before loading JSON
            "reason": reason,
            "timestamp": time.time()
        }
        # producer expects bytes, our serializer makes it bytes
        kafka_producer.send(KAFKA_TOPIC_DLQ, value=dlq_message)
        logger.info(f"Sent message to DLQ ({KAFKA_TOPIC_DLQ}) due to: {reason}")
    except Exception as e:
        logger.error(f"Failed to send message to DLQ: {e}")
        # At this point, the message is likely lost unless you have other mechanisms
        # Consider logging the message content to a file as a last resort
        logger.error(f"Original message that failed to go to DLQ: {original_payload}")


def run_router():
    """Main function to connect to services and start consuming Kafka messages."""
    logger.info("Starting Analytics Router...")
    connect_to_services()

    logger.info(f"Listening for messages on Kafka topic: {KAFKA_TOPIC_RAW}")

    # Process messages in batches for efficiency (adjust batch size as needed)
    # The kafka-python consumer provides messages in batches when iterating
    for message in kafka_consumer:
        logger.info(f"Processing message from topic {message.topic}, partition {message.partition}, offset {message.offset}")
        # In a real batch processing loop, you'd process a batch of messages
        # and then commit offsets for the entire batch.
        # For simplicity in this placeholder, we process one by one and commit per message
        # (less efficient but easier to demonstrate).

        processed_successfully = process_event(message)

        if processed_successfully:
             logger.info(f"Message {message.offset} processed successfully.")
             # Manually commit offset after successful processing
             kafka_consumer.commit()
             logger.debug(f"Committed offset {message.offset} for partition {message.partition}")
        else:
             logger.warning(f"Message {message.offset} failed processing. Sent to DLQ.")
             # Do NOT commit offset if processing failed and sent to DLQ.
             # The message will be re-delivered on consumer restart/rebalance.


if __name__ == "__main__":
    # Implement a basic HTTP server for health check if needed
    # from http.server import BaseHTTPRequestHandler, HTTPServer
    # class HealthCheckHandler(BaseHTTPRequestHandler):
    #     def do_GET(self):
    #         if self.path == '/healthz':
    #             self.send_response(200)
    #             self.end_headers()
    #             self.wfile.write(b'OK')
    #         else:
    #             self.send_response(404)
    #             self.end_headers()
    # server = HTTPServer(('0.0.0.0', ROUTER_LISTEN_PORT), HealthCheckHandler)
    # # Run the health check server in a separate thread or process
    # import threading
    # health_thread = threading.Thread(target=server.serve_forever)
    # health_thread.daemon = True # Allow main thread to exit even if health thread is running
    # health_thread.start()
    # logger.info(f"Health check server listening on port {ROUTER_LISTEN_PORT}")


    # Run the main router logic
    try:
        run_router()
    except KeyboardInterrupt:
        logger.info("Shutting down router.")
        if kafka_consumer:
            kafka_consumer.close()
        if kafka_producer:
             kafka_producer.close()
        # Close other connections if they were real
        # if redis_client: redis_client.close()
        # if pg_conn_pool: pg_conn_pool.closeall()
        # if ch_client: ch_client.close()
    except Exception as e:
        logger.critical(f"Router crashed due to an unhandled error: {e}", exc_info=True)
        # Log the error and exit. Container orchestrator will restart.
        os._exit(1) # Exit with non-zero status