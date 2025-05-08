import json
from kafka import KafkaConsumer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC_DLQ = 'shared_dead_letter'

consumer = None
try:
    # Consume from earliest offset, group ID is arbitrary for simple check
    consumer = KafkaConsumer(
        KAFKA_TOPIC_DLQ,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True, # Auto commit for simple check
        group_id='dlq_checker'
    )
    print(f"Waiting for messages on topic {KAFKA_TOPIC_DLQ}...")
    # Poll for messages for a short duration
    messages = consumer.poll(timeout_ms=5000, max_records=10) # Poll for 5 seconds, max 10 records

    if not messages:
        print("No messages received.")
    else:
        for tp, records in messages.items():
             for record in records:
                  print(f"Received message: {record.value.decode('utf-8')}") # Decode bytes to string

except Exception as e:
    print(f"Error consuming messages: {e}")
finally:
    if consumer:
        consumer.close() 