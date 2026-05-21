package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamProcessor {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab11-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Read from the input topic
        KStream<String, String> sourceStream = builder.stream("stream-input-topic");

        // 2. Perform a transformation: Convert value to uppercase
        KStream<String, String> uppercasedStream = sourceStream.mapValues(value -> {
            System.out.println("Processing: " + value);
            return value != null ? value.toUpperCase() : null;
        });

        // 3. Write the transformed data to the output topic
        uppercasedStream.to("stream-output-topic");

        Topology topology = builder.build();
        System.out.println("Starting Kafka Streams Application...");
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        // Attach shutdown handler to catch control-c
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
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
