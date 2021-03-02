package io.micronaut.configuration.kafka

import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared

abstract class AbstractEmbeddedServerSpec extends AbstractKafkaSpec {

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()
    @Shared @AutoCleanup EmbeddedServer embeddedServer
    @Shared @AutoCleanup ApplicationContext context

    void setupSpec() {
        kafkaContainer.start()
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                getConfiguration() +
                        ['kafka.bootstrap.servers': kafkaContainer.bootstrapServers]
        )
        context = embeddedServer.applicationContext
    }
}
