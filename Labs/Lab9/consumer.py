#!/usr/bin/env python3
"""
Avro Consumer - Consumes records using Avro schema from Confluent Schema Registry
Reads schema from external file.
"""

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import json
import signal
import sys
import time

# Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'avro-topic'
GROUP_ID = f'avro-consumer-group-{int(time.time())}'
SCHEMA_FILE = 'order_schema_v1.avsc'

def load_schema(schema_file):
    """Load Avro schema from .avsc file"""
    try:
        with open(schema_file, 'r') as f:
            schema_str = f.read()
        print(f"✓ Schema loaded from {schema_file}")
        return schema_str
    except FileNotFoundError:
        print(f"✗ Error: Schema file '{schema_file}' not found")
        sys.exit(1)


def main():
    
    # Load schema from file
    schema_str = load_schema(SCHEMA_FILE)
    
    # Initialize Schema Registry Client
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    
    # Create Avro Deserializer
    avro_deserializer = AvroDeserializer(schema_registry_client)
    string_deserializer = StringDeserializer('utf_8')
    
    # Create Consumer
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    })
    
    consumer.subscribe([TOPIC])
    
    print(f"\n{'='*60}")
    print(f"AVRO CONSUMER - Externalized Configuration")
    print(f"{'='*60}")
    print(f"Kafka Broker: {BOOTSTRAP_SERVERS}")
    print(f"Topic: {TOPIC}")
    print(f"Consumer Group: {GROUP_ID}")
    print(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"Schema File: {SCHEMA_FILE}")
    print(f"{'='*60}")
    print("Waiting for messages...\n")
    
    message_count = 0
    no_message_count = 0
    start_time = time.time()
    max_wait_time = 12  # Wait max 12 seconds for messages
    
    try:
        while True:
            # Exit if we've been waiting too long with no messages
            if time.time() - start_time > max_wait_time:
                if message_count == 0:
                    print(f"\n⏱️ No messages received after {max_wait_time}s - exiting")
                else:
                    print(f"\n✓ Finished consuming {message_count} messages")
                break
            
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                no_message_count += 1
                # Print progress indicator if no messages
                if no_message_count % 3 == 0:
                    elapsed = int(time.time() - start_time)
                    print(f"  ({elapsed}s elapsed, waiting for messages...)")
                continue
            
            # Reset counter when we get a message
            no_message_count = 0
            
            if msg.error():
                print(f'Error: {msg.error()}')
                continue
            
            # Deserialize the message
            key = string_deserializer(msg.key())
            try:
                value = avro_deserializer(msg.value())
            except Exception as e:
                print(f"Error deserializing message: {e}")
                continue
            
            message_count += 1
            print(f"\n[Message {message_count}]")
            print(f"Partition: {msg.partition()} | Offset: {msg.offset()}")
            print(f"Key: {key}")
            print(f"Order Details:")
            print(f"  ├─ Order ID: {value.get('order_id')}")
            print(f"  ├─ Customer: {value.get('customer_name')}")
            print(f"  ├─ Product: {value.get('product')}")
            print(f"  ├─ Quantity: {value.get('quantity')}")
            print(f"  └─ Price: ${value.get('price'):.2f}")
            print("-" * 60)
            
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
