package io.micronaut.testcontainers.kafka;

import org.testcontainers.kafka.KafkaContainer;

import java.util.HashMap;
import java.util.Map;

public class Kafka {

    private static final String IMAGE_NAME = "apache/kafka-native";
    private static KafkaContainer container;

    public static Map<String, String> getProperties() {
        if (container == null) {
            container = new KafkaContainer(IMAGE_NAME);
            container.start();
            do {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            } while(!container.isRunning());
            return getProperties(container);
        } else {
            return getProperties(container);
        }
    }

    private static Map<String, String> getProperties(KafkaContainer container) {
        final Map<String, String> map = new HashMap<>(Map.of(
            "kafka.bootstrap.servers", container.getBootstrapServers()
        ));
        System.out.println("map = " + map);
        return map;
    }
}
