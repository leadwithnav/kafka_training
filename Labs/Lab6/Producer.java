import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        // 1. Configure the Producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Equivalent to StringSerializer('utf_8') in Python
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Create the Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        String topic = "orders";
        String csvFilePath = "orders.csv";

        System.out.println("Starting to produce messages to topic: " + topic);

        // 3. Read orders from the CSV file
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            boolean isFirstLine = true;
            String[] headers = null;

            while ((line = br.readLine()) != null) {
                // Skip the header row but save it to build our JSON
                if (isFirstLine) {
                    headers = line.split(",");
                    isFirstLine = false;
                    continue;
                }

                String[] values = line.split(",");
                if (values.length != headers.length) continue;

                // Build a simple JSON string manually from the CSV row
                StringBuilder jsonBuilder = new StringBuilder();
                jsonBuilder.append("{");
                for (int i = 0; i < headers.length; i++) {
                    jsonBuilder.append("\"").append(headers[i]).append("\":\"").append(values[i]).append("\"");
                    if (i < headers.length - 1) {
                        jsonBuilder.append(",");
                    }
                }
                jsonBuilder.append("}");

                // Key is the order_id (first column)
                String key = values[0]; 
                String value = jsonBuilder.toString();

                // 4. Create a ProducerRecord (equivalent to producer.produce arguments)
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // 5. Asynchronously send the message with a delivery callback
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("Message delivery failed: " + exception.getMessage());
                        } else {
                            System.out.println("Delivered to " + metadata.topic() + " [" + metadata.partition() + "] at offset " + metadata.offset() + " (Key: " + key + ")");
                        }
                    }
                });
            }
        } catch (IOException e) {
            System.err.println("Error reading " + csvFilePath + ": " + e.getMessage());
            System.exit(1);
        }

        // 6. Wait for all messages in the buffer to be delivered
        System.out.println("Waiting for all messages to be delivered...");
        producer.flush();
        producer.close();
        
        System.out.println("Producer finished.");
    }
}
