package io.micronaut.kafka.docs

import io.micronaut.test.support.TestPropertyProvider
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Specification

abstract class AbstractKafkaTest extends Specification implements TestPropertyProvider {

    static final KafkaContainer MY_KAFKA

    static {
        MY_KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
        MY_KAFKA.start()
    }

    @Override
    Map<String, String> getProperties() {
        return Collections.singletonMap(
                "kafka.bootstrap.servers", MY_KAFKA.getBootstrapServers()
        )
    }
}
