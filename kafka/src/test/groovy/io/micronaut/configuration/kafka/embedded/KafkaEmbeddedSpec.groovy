package io.micronaut.configuration.kafka.embedded

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration
import io.micronaut.context.ApplicationContext
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import spock.lang.AutoCleanup
import spock.lang.Specification

import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class KafkaEmbeddedSpec extends Specification {

    @AutoCleanup ApplicationContext applicationContext

    void "test run kafka embedded server"() {
        given:
        run()

        when:
        AbstractKafkaConsumerConfiguration config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] == AbstractKafkaConfiguration.DEFAULT_BOOTSTRAP_SERVERS

        when:
        KafkaEmbedded kafkaEmbedded = applicationContext.getBean(KafkaEmbedded)

        then:
        kafkaEmbedded.kafkaServer.isPresent()
        kafkaEmbedded.zkPort.isPresent()
    }

    void "test run kafka embedded server with multi partition topic"() {
        given:
        int partitionNumber = 10
        String topicName = "multi-partition-topic"

        run(
            (EMBEDDED_TOPICS): topicName,
            "kafka.embedded.properties.num.partitions": partitionNumber
        )

        AdminClient adminClient = createAdminClient()

        when:
        KafkaEmbedded kafkaEmbedded = applicationContext.getBean(KafkaEmbedded)

        then:
        kafkaEmbedded.kafkaServer.isPresent()
        kafkaEmbedded.zkPort.isPresent()

        and:
        adminClient
                .describeTopics([topicName]).values()
                .get(topicName).get()
                .partitions().size() == partitionNumber

        cleanup:
        adminClient.close()
    }

    void "test run kafka embedded server with single partition topic"() {
        given:
        String topicName = "single-partition-topic"

        run((EMBEDDED_TOPICS): topicName)

        AdminClient adminClient = createAdminClient()

        when:
        KafkaEmbedded kafkaEmbedded = applicationContext.getBean(KafkaEmbedded)

        then:
        kafkaEmbedded.kafkaServer.isPresent()
        kafkaEmbedded.zkPort.isPresent()

        and:
        adminClient
                .describeTopics([topicName]).values()
                .get(topicName).get()
                .partitions().size() == 1

        cleanup:
        adminClient.close()
    }

    private static AdminClient createAdminClient() {
        AdminClient.create((CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG): AbstractKafkaConfiguration.DEFAULT_BOOTSTRAP_SERVERS)
    }

    private void run(Map<String, Object> extraProps = [:]) {
        applicationContext = ApplicationContext.run(
                [(EMBEDDED): true] + extraProps
        )
    }
}
