package io.micronaut.configuration.kafka.streams

import io.micronaut.context.ApplicationContext
import spock.lang.AutoCleanup
import spock.lang.Shared

abstract class AbstractKafkaContainerSpec extends AbstractKafkaSpec {

    @Shared @AutoCleanup ApplicationContext context
    @Shared String bootstrapServers

    void setupSpec() {
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

    void cleanupSpec() {
        stopContext()
    }
}
