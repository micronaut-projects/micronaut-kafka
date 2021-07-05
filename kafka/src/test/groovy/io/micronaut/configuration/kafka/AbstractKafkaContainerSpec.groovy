package io.micronaut.configuration.kafka

import io.micronaut.context.ApplicationContext
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared

abstract class AbstractKafkaContainerSpec extends AbstractKafkaSpec {

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer().withEnv(getEnvVariables())
    @Shared @AutoCleanup ApplicationContext context

    void setupSpec() {
        kafkaContainer.start()
        context = ApplicationContext.run(
                getConfiguration() +
                        ['kafka.bootstrap.servers': kafkaContainer.bootstrapServers]
        )
    }

    protected Map<String, String> getEnvVariables() {
        return [:]
    }

}
