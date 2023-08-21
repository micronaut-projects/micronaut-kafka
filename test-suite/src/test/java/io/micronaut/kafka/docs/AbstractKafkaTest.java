package io.micronaut.kafka.docs;

import io.micronaut.test.support.TestPropertyProvider;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Map;

/**
 * @see <a href="https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers">Singleton containers</a>
 */
public abstract class AbstractKafkaTest implements TestPropertyProvider {

    static protected final KafkaContainer MY_KAFKA = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:latest")
    );

    @Override
    public Map<String, String> getProperties() {
        if (!MY_KAFKA.isRunning()) {
            MY_KAFKA.start();
        }
        return Collections.singletonMap(
            "kafka.bootstrap.servers", MY_KAFKA.getBootstrapServers()
        );
    }
}
