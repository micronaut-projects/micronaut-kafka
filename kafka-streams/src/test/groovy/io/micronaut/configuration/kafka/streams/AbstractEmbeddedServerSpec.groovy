package io.micronaut.configuration.kafka.streams

import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.AutoCleanup
import spock.lang.Shared

abstract class AbstractEmbeddedServerSpec extends AbstractKafkaContainerSpec {

    @Shared @AutoCleanup EmbeddedServer embeddedServer

    @Override
    void startContext() {
        embeddedServer = ApplicationContext.builder(getConfiguration()).bootstrapEnvironment(true).run(EmbeddedServer.class);
        context = embeddedServer.getApplicationContext();
        bootstrapServers = context.getRequiredProperty("kafka.bootstrap.servers", String.class);
    }

}
