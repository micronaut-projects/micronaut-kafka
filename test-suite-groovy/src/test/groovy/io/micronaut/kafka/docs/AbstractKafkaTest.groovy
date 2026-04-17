package io.micronaut.kafka.docs

import io.micronaut.test.support.TestPropertyProvider
import org.testcontainers.kafka.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

abstract class AbstractKafkaTest extends Specification implements TestPropertyProvider {

    @Shared
    @AutoCleanup
    KafkaContainer kafkaContainer = new KafkaContainer("apache/kafka:4.2.0")

    @Override
    Map<String, String> getProperties() {
        kafkaContainer.start()

        ["kafka.bootstrap.servers": kafkaContainer.getBootstrapServers()]
    }
}
