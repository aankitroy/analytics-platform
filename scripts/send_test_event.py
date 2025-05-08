import json
import uuid
import time
import hashlib # For hashing the API key
from kafka import KafkaProducer

# Kafka broker address on your host
# This maps to the kafka:9092 port exposed in docker-compose
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'shared_raw_events'

# --- Test Client Info ---
# Use the same test client details as in your 02_insert_test_client.sql
TEST_CLIENT_ID = 'a1b2c3d4-e5f6-7890-1234-567890abcdef'
TEST_API_KEY = 'test_api_key_a950e78aa1d48bfb180f99adeae4aee3' # The raw API key
# Hash the API key using the same method you would in your Router and provisioning process
TEST_API_KEY_HASH = hashlib.sha256(TEST_API_KEY.encode('utf-8')).hexdigest()

# --- Create a Test Event Payload ---
test_event = {
    # Include client info in the event payload as expected by your Router
    'client_id': TEST_CLIENT_ID,
    'api_key_hash': TEST_API_KEY_HASH, # Router validates against this hash
    # 'api_key': TEST_API_KEY, # **Do NOT send raw API keys in real events!**

    'event_id': str(uuid.uuid4()), # Unique ID for this event
    'event_time': int(time.time()), # Timestamp in milliseconds (adjust format if needed)
    'event_name': 'user_signup', # Example event name
    'event_properties': { # Nested JSON properties
        'plan': 'free',
        'source': 'website',
        'browser': 'chrome'
    },
    'user_id': 'user_abc123', # Example user identifier
    'timestamp': int(time.time()) # Another timestamp field (common)
}

# Create a Kafka producer
producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8') # Serialize dictionary to JSON bytes
    )

    # Send the test event
    future = producer.send(KAFKA_TOPIC, value=test_event)
    result = future.get(timeout=10) # Wait for send to finish

    print(f"Sent message to topic {result.topic}, partition {result.partition}, offset {result.offset}")

except Exception as e:
    print(f"Failed to send message: {e}")
finally:
    if producer:
        producer.close() # Close the producer connection 