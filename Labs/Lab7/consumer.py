from confluent_kafka import Consumer, KafkaError

def main():
    # 1. Configure the Consumer properties
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'python-orders-consumer-group',
        'auto.offset.reset': 'earliest' # Read from beginning if no offset exists
    }

    # 2. Create the Kafka Consumer
    consumer = Consumer(conf)

    # 3. Subscribe to the topic
    topic = 'orders'
    consumer.subscribe([topic])
    print(f"Subscribed to topic: {topic}")
    print("Waiting for messages...")

    # 4. Poll for new data
    # For this lab, we will stop polling after 5 consecutive seconds of no messages
    # so that the Jupyter Notebook cell finishes successfully!
    empty_poll_count = 0
    max_empty_polls = 5

    try:
        while empty_poll_count < max_empty_polls:
            # Poll the broker for new data (wait up to 1000ms)
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                empty_poll_count += 1
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Reset the count since we got data!
            empty_poll_count = 0 

            # Process the records
            val = msg.value().decode('utf-8') if msg.value() else "None"
            key = msg.key().decode('utf-8') if msg.key() else "None"
            print(f"Received Order -> Key: {key} | Value: {val} | Partition: {msg.partition()} | Offset: {msg.offset()}")

    except Exception as e:
        print(f"Error during consumption: {e}")
    finally:
        print(f"No more messages received for {max_empty_polls} seconds. Shutting down.")
        consumer.close() # Always close the consumer to commit offsets and leave the group gracefully!

if __name__ == '__main__':
    main()
