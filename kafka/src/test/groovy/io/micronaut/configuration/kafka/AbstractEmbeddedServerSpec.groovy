package io.micronaut.configuration.kafka

import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.AutoCleanup
import spock.lang.Shared

abstract class AbstractEmbeddedServerSpec extends AbstractKafkaContainerSpec {

    @Shared @AutoCleanup EmbeddedServer embeddedServer

    @Override
    void startContext() {
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                getConfiguration()
        )
        context = embeddedServer.applicationContext
        bootstrapServers = context.getRequiredProperty("kafka.bootstrap.servers", String.class);
    }

}
