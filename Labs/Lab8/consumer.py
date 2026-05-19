import os
import sys
from confluent_kafka import Consumer, KafkaError

def main():
    # We use the OS process ID to give this consumer a unique name in our log files!
    consumer_name = f"C-{os.getpid()}"
    
    # --- LAB INSTRUCTION: You will change this to 'group-B' later in the lab! ---
    group_id = 'group-A'

    # Configure the consumer
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe(['group-topic'])

    print(f"Consumer {consumer_name} joined group: {group_id}")
    sys.stdout.flush() # Force output to flush immediately so our log files update in real-time

    try:
        while True:
            # Poll for new messages (wait up to 1 second)
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            val = msg.value().decode('utf-8') if msg.value() else "None"
            print(f"[{consumer_name}] Processed Order -> Partition: {msg.partition()} | Offset: {msg.offset()} | Value: {val}")
            sys.stdout.flush()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
