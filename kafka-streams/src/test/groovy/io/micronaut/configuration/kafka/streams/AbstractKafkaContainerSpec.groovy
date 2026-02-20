package io.micronaut.configuration.kafka.streams

import io.micronaut.context.ApplicationContext
import spock.lang.AutoCleanup
import spock.lang.Shared

import org.testcontainers.kafka.KafkaContainer

abstract class AbstractKafkaContainerSpec extends AbstractKafkaSpec {

    @Shared @AutoCleanup ApplicationContext context
    @Shared String bootstrapServers
    @Shared @AutoCleanup KafkaContainer kafkaContainer

    void setupSpec() {
        kafkaContainer = new KafkaContainer("apache/kafka-native")
        kafkaContainer.start()

        startContext()
        afterKafkaStarted()
    }

    void afterKafkaStarted() {
    }

    void startContext() {
        context = ApplicationContext.run(
                getConfiguration()
        )
        bootstrapServers = context.getRequiredProperty("kafka.bootstrap.servers", String.class)
    }

    void stopContext() {
        context?.stop()
    }

    protected Map<String, Object> getConfiguration() {
        def config = super.getConfiguration()
        config['kafka.bootstrap.servers'] = kafkaContainer.getBootstrapServers()
        return config
    }

    void cleanupSpec() {
        stopContext()
    }
}
