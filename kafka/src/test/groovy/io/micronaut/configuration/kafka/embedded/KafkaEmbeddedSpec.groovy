
package io.micronaut.configuration.kafka.embedded

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration
import io.micronaut.context.ApplicationContext
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import spock.lang.Specification

class KafkaEmbeddedSpec extends Specification {

    void "test run kafka embedded server"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                Collections.singletonMap(
                        AbstractKafkaConfiguration.EMBEDDED, true
                )
        )

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

        cleanup:
        applicationContext.close()
    }

    void "test run kafka embedded server with multi partition topic"() {
        given:
        int partitionNumber = 10
        String topicName = "multi-partition-topic"

        ApplicationContext applicationContext = ApplicationContext.run(
                [
                        (AbstractKafkaConfiguration.EMBEDDED)       : true,
                        (AbstractKafkaConfiguration.EMBEDDED_TOPICS): topicName,
                        "kafka.embedded.properties.num.partitions"  : partitionNumber
                ]
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
        applicationContext.close()
    }

    void "test run kafka embedded server with single partition topic"() {
        given:
        String topicName = "single-partition-topic"

        ApplicationContext applicationContext = ApplicationContext.run(
                [
                        (AbstractKafkaConfiguration.EMBEDDED)       : true,
                        (AbstractKafkaConfiguration.EMBEDDED_TOPICS): topicName
                ]
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
                .partitions().size() == 1


        cleanup:
        adminClient.close()
        applicationContext.close()
    }

    private static AdminClient createAdminClient() {
        Properties properties = new Properties()
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, AbstractKafkaConfiguration.DEFAULT_BOOTSTRAP_SERVERS)
        AdminClient adminClient = AdminClient.create(properties)
        adminClient
    }
}
