package io.micronaut.kafka.docs

import io.micronaut.test.support.TestPropertyProvider
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Specification

/**
 * @see <a href="https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers">Singleton containers</a>
 */
abstract class AbstractKafkaTest extends Specification implements TestPropertyProvider {

    static final KafkaContainer MY_KAFKA

    static {
        MY_KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
        MY_KAFKA.start()
    }

    @Override
    Map<String, String> getProperties() {
        while (!MY_KAFKA.isRunning()) {
            MY_KAFKA.start()
        }
        ["kafka.bootstrap.servers": MY_KAFKA.getBootstrapServers()]
    }
}
