import csv
import json
import sys
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        # For SerializingProducer, msg.key() and msg.value() return the serialized bytes
        print(f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()} (Key: {msg.key().decode('utf-8')})")

def main():
    # Configure the Serializing Producer
    # We define BOTH key and value to use the standard StringSerializer
    string_serializer = StringSerializer('utf_8')

    conf = {
        'bootstrap.servers': 'localhost:9092',
        'key.serializer': string_serializer,
        'value.serializer': string_serializer
    }
    
    try:
        producer = SerializingProducer(conf)
    except Exception as e:
        print(f"Failed to create producer: {e}")
        sys.exit(1)

    topic = 'orders'
    csv_file_path = 'orders.csv'

    print(f"Starting to produce messages to topic: {topic}")

    # Read orders from CSV
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                # Key is the order_id (string)
                key = row['order_id']
                
                # Value is the row converted into a JSON string BEFORE sending
                value = json.dumps(row)
                
                # The SerializingProducer automatically applies the StringSerializer to both!
                producer.produce(topic, key=key, value=value, on_delivery=delivery_report)
                
                # Trigger any available delivery report callbacks from previous produce() calls
                producer.poll(0)
                
    except FileNotFoundError:
        print(f"Error: {csv_file_path} not found.")
        sys.exit(1)

    # Wait for any outstanding messages to be delivered
    print("Waiting for all messages to be delivered...")
    producer.flush()
    print("Producer finished.")

if __name__ == '__main__':
    main()
