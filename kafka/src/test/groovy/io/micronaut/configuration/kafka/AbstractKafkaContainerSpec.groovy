package io.micronaut.configuration.kafka

import io.micronaut.context.ApplicationContext
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
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
        bootstrapServers = context.getRequiredProperty("kafka.bootstrap.servers", String.class);
    }

    void createTopic(String name, int numPartitions, int replicationFactor) {
        try (def admin = AdminClient.create([(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG): bootstrapServers])) {
            admin.createTopics([new NewTopic(name, numPartitions, (short) replicationFactor)]).all().get()
        }
    }

    protected Map<String, String> getEnvVariables() {
        [:]
    }
}
