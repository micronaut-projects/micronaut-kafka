package io.micronaut.configuration.kafka

import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared

import java.util.stream.Collectors

abstract class AbstractEmbeddedServerSpec extends AbstractKafkaSpec {

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()
    @Shared @AutoCleanup EmbeddedServer embeddedServer
    @Shared @AutoCleanup ApplicationContext context

    void setupSpec() {
        startKafka()
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                getConfiguration() +
                        ['kafka.bootstrap.servers': kafkaContainer.bootstrapServers]
        )
        context = embeddedServer.applicationContext
    }

    void startKafka() {
        kafkaContainer.start()
    }

    void cleanupSpec() {
        context.stop()
        kafkaContainer.stop()
    }

    void createTopic(String name, int numPartitions, int replicationFactor) {
        try (def admin = AdminClient.create([(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG): kafkaContainer.bootstrapServers])) {
            admin.createTopics([new NewTopic(name, numPartitions, (short) replicationFactor)]).all().get()
        }
    }
}
