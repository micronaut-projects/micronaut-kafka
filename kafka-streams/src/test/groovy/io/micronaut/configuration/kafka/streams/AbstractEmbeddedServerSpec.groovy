package io.micronaut.configuration.kafka.streams

import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.AutoCleanup
import spock.lang.Shared

abstract class AbstractEmbeddedServerSpec extends AbstractKafkaContainerSpec {

    @Shared @AutoCleanup EmbeddedServer embeddedServer

    @Override
    void startContext() {
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                getConfiguration() +
                        ['kafka.bootstrap.servers': kafkaContainer.bootstrapServers]
        )
        context = embeddedServer.applicationContext
    }

}
