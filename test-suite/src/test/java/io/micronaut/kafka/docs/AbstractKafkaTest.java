package io.micronaut.kafka.docs;

import io.micronaut.test.support.TestPropertyProvider;
import org.testcontainers.kafka.KafkaContainer;

import java.util.Map;

/**
 * @see <a href="https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers">Singleton containers</a>
 */
public abstract class AbstractKafkaTest implements TestPropertyProvider {

    static final KafkaContainer MY_KAFKA = new KafkaContainer("apache/kafka:4.2.0");

    @Override
    public Map<String, String> getProperties() {
        if (!MY_KAFKA.isRunning()) {
            MY_KAFKA.start();
        }
        return Map.of(
            "kafka.bootstrap.servers", MY_KAFKA.getBootstrapServers()
        );
    }
}
