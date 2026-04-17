package io.micronaut.configuration.kafka

import io.micronaut.context.ApplicationContext
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.testcontainers.kafka.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared

abstract class AbstractKafkaContainerSpec extends AbstractKafkaSpec {

    @Shared @AutoCleanup ApplicationContext context
    @Shared String bootstrapServers
    @Shared @AutoCleanup KafkaContainer kafkaContainer

    void setupSpec() {
        kafkaContainer = new KafkaContainer("apache/kafka:4.2.0")
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
        bootstrapServers = context.getRequiredProperty("kafka.bootstrap.servers", String.class);
    }

    protected Map<String, Object> getConfiguration() {
        def config = super.getConfiguration()
        config['kafka.bootstrap.servers'] = kafkaContainer.getBootstrapServers()
        config
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
