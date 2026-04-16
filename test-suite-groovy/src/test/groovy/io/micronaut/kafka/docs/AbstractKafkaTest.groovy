package io.micronaut.kafka.docs

import io.micronaut.test.support.TestPropertyProvider
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

abstract class AbstractKafkaTest extends Specification implements TestPropertyProvider {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("apache/kafka:4.2.0")

    @Shared
    @AutoCleanup
    KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_IMAGE)

    @Override
    Map<String, String> getProperties() {
        kafkaContainer.start()

        ["kafka.bootstrap.servers": kafkaContainer.getBootstrapServers()]
    }
}
