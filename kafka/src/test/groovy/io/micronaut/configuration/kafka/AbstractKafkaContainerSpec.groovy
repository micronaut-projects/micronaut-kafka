package io.micronaut.configuration.kafka

import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import io.micronaut.context.ApplicationContext
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import spock.lang.AutoCleanup
import spock.lang.Shared

abstract class AbstractKafkaContainerSpec extends AbstractKafkaSpec {

    @Shared @AutoCleanup ApplicationContext context
    @Shared String bootstrapServers
    @Shared @AutoCleanup KafkaContainer kafkaContainer

    void setupSpec() {
        def kafkaImage = DockerImageName
                .parse("apache/kafka:3.9.1")
        kafkaContainer = new KafkaContainer(kafkaImage)
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
