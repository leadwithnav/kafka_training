package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Joined;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamJoinProcessor {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab12-stream-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Required for table materialization state directory
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Read users data as a KTable (Reference Data)
        KTable<String, String> userTable = builder.table("users-topic");

        // 2. Read clicks data as a KStream (Event Data)
        KStream<String, String> clickStream = builder.stream("clicks-topic");

        // 3. Join the KStream with the KTable
        // The join is implicitly based on the same key (userId)
        KStream<String, String> enrichedClicks = clickStream.join(
            userTable,
            (clickUrl, region) -> {
                System.out.println("Joining click: " + clickUrl + " with region: " + region);
                return clickUrl + " | Region=" + region;
            },
            Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );

        // 4. Transformation: Filter out non-US users
        KStream<String, String> usOnlyClicks = enrichedClicks.filter((userId, enrichedData) -> {
            boolean isUS = enrichedData.contains("Region=US");
            if (isUS) {
                System.out.println("Allowed US Click: " + enrichedData);
            } else {
                System.out.println("Filtered out non-US Click: " + enrichedData);
            }
            return isUS;
        });

        // 5. Transformation: Map the final string for output
        KStream<String, String> finalStream = usOnlyClicks.mapValues(value -> "[ENRICHED] " + value);

        // 6. Output to sink topic
        finalStream.to("enriched-clicks-topic");

        Topology topology = builder.build();
        System.out.println("Starting Kafka Streams Application...");
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        // Attach shutdown handler
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Stopping Streams Application...");
                streams.close();
                latch.countDown();
            }
        });

        try {
            // Clean up local state since we might restart the app during testing
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
