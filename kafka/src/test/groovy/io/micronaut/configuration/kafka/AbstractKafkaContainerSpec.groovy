package io.micronaut.configuration.kafka

import io.micronaut.context.ApplicationContext
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.AutoCleanup
import spock.lang.Shared

abstract class AbstractKafkaContainerSpec extends AbstractKafkaSpec {

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.4")).withEnv(getEnvVariables())
    @Shared @AutoCleanup ApplicationContext context

    void setupSpec() {
        kafkaContainer.start()
        afterKafkaStarted()
        startContext()
    }

    void afterKafkaStarted() {
    }

    void startContext() {
        context = ApplicationContext.run(
                getConfiguration() +
                        ['kafka.bootstrap.servers': kafkaContainer.bootstrapServers]
        )
    }

    void stopContext() {
        context.stop()
    }

    void cleanupSpec() {
        kafkaContainer.stop()
    }

    void createTopic(String name, int numPartitions, int replicationFactor) {
        try (def admin = AdminClient.create([(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG): kafkaContainer.bootstrapServers])) {
            admin.createTopics([new NewTopic(name, numPartitions, (short) replicationFactor)]).all().get()
        }
    }

    protected Map<String, String> getEnvVariables() {
        [:]
    }
}
