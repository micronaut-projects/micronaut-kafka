package io.micronaut.testcontainers.kafka;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class Kafka {

    private static final String IMAGE_NAME = "apache/kafka-native:3.8.0";
    private static KafkaContainer container;

    public static Map<String, String> getProperties() {
        if (container == null) {
            container = new KafkaContainer(DockerImageName.parse(IMAGE_NAME));
            container.start();
            do {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } while(!container.isRunning());
            return getProperties(container);
        } else {
            return getProperties(container);
        }
    }

    private static Map<String, String> getProperties(KafkaContainer container) {
        return Map.of(
            "kafka.bootstrap.servers", container.getBootstrapServers()
        );
    }
}
