package io.micronaut.kafka.docs

import io.micronaut.test.support.TestPropertyProvider
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

abstract class AbstractKafkaTest extends Specification implements TestPropertyProvider {

    @Shared
    @AutoCleanup
    KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))

    @Override
    Map<String, String> getProperties() {
        kafkaContainer.start()

        ["kafka.bootstrap.servers": kafkaContainer.getBootstrapServers()]
    }
}
