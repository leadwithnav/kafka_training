#!/usr/bin/env python3
"""
Avro Producer - Produces records using Avro schema registered in Confluent Schema Registry
Reads schema and data from external files.
"""

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import json
import csv
import sys
import time
import os

# Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'avro-topic'
SCHEMA_FILE = 'order_schema_v1.avsc'
DATA_FILE = 'orders.csv'

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

def load_data(data_file):
    """Load order data from CSV file"""
    try:
        orders = []
        with open(data_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                order = {
                    'order_id': row['order_id'],
                    'customer_name': row['customer_name'],
                    'product': row['product'],
                    'quantity': int(row['quantity']),
                    'price': float(row['price']),
                    'timestamp': int(time.time() * 1000)
                }
                orders.append(order)
        print(f"✓ Data loaded from {data_file} ({len(orders)} records)")
        return orders
    except FileNotFoundError:
        print(f"✗ Error: Data file '{data_file}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error reading data file: {e}")
        sys.exit(1)

# Track delivery status globally
delivered_messages = 0
failed_messages = 0
delivery_errors = []

def delivery_report(err, msg):
    """Delivery report callback called for each message produced"""
    global delivered_messages, failed_messages, delivery_errors
    if err is not None:
        failed_messages += 1
        delivery_errors.append(f'{msg.key().decode() if msg.key() else "unknown"}: {err}')
        print(f'  ✗ Delivery failed: {err}')
    else:
        delivered_messages += 1
        print(f'  ✓ Delivered to partition {msg.partition()} at offset {msg.offset()}')

def main():
    global delivered_messages, failed_messages, delivery_errors
    
    # Load schema and data from files
    schema_str = load_schema(SCHEMA_FILE)
    orders = load_data(DATA_FILE)
    
    # Initialize Schema Registry Client
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    
    # Create Avro Serializers
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    string_serializer = StringSerializer('utf_8')
    
    # Create Producer
    producer = Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'enable.idempotence': True,
        'acks': 'all'
    })
    
    # Produce messages
    print(f"\nProducing {len(orders)} messages...")
    print("-" * 60)
    for order in orders:
        try:
            print(f"Producing: {order['order_id']} - {order['customer_name']}")
            producer.produce(
                topic=TOPIC,
                key=string_serializer(order['order_id']),
                value=avro_serializer(order, SerializationContext(TOPIC, MessageField.VALUE)),
                on_delivery=delivery_report
            )
            time.sleep(0.2)
        except Exception as e:
            print(f"  ✗ Error producing message: {e}")
            failed_messages += 1
    
    # Wait for pending messages to be delivered
    print("-" * 60)
    print("Flushing messages to Kafka...")
    remaining = producer.flush(timeout=10)
    
    if remaining > 0:
        print(f"  ⚠️  {remaining} messages were not delivered within timeout")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"PRODUCTION SUMMARY")
    print(f"{'='*60}")
    print(f"✓ Messages delivered: {delivered_messages}/{len(orders)}")
    if failed_messages > 0:
        print(f"✗ Messages failed: {failed_messages}")
        for error in delivery_errors:
            print(f"    - {error}")
    print(f"✓ Schema registered in Schema Registry")
    print(f"{'='*60}")
    
    if delivered_messages > 0:
        print(f"✅ SUCCESS: {delivered_messages} messages are in the topic!")
    else:
        print(f"❌ FAILURE: No messages were delivered!")
        sys.exit(1)

if __name__ == '__main__':
    main()
