package io.micronaut.configuration.kafka

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration
import io.micronaut.configuration.kafka.config.KafkaConsumerConfiguration
import io.micronaut.context.ApplicationContext
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.serialization.StringDeserializer
import spock.lang.Specification

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG

class KafkaConfigurationSpec extends Specification {

    void "test default consumer configuration"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                ("kafka." + KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka." + VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name
        )

        when:
        AbstractKafkaConsumerConfiguration config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == AbstractKafkaConfiguration.DEFAULT_BOOTSTRAP_SERVERS

        when:
        Consumer consumer = applicationContext.createBean(Consumer, config)

        then:
        consumer != null

        cleanup:
        consumer.close()
        applicationContext.close()
    }

    void "test configure default properties"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                ("kafka.${BOOTSTRAP_SERVERS_CONFIG}".toString()):"localhost:1111",
                ("kafka.${GROUP_ID_CONFIG}".toString()):"mygroup",
                ("kafka.${MAX_POLL_RECORDS_CONFIG}".toString()):"100",
                ("kafka." + KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka." + VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name
        )

        when:
        AbstractKafkaConsumerConfiguration config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == "localhost:1111"
        props[GROUP_ID_CONFIG] == "mygroup"
        props[MAX_POLL_RECORDS_CONFIG] == "100"

        when:
        Consumer consumer = applicationContext.createBean(Consumer, config)

        then:
        consumer != null

        cleanup:
        consumer.close()
        applicationContext.close()
    }

    void "test override consumer default properties"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                ("kafka.${BOOTSTRAP_SERVERS_CONFIG}".toString()):"localhost:1111",
                ("kafka.${GROUP_ID_CONFIG}".toString()):"mygroup",
                ("kafka.consumers.default.${BOOTSTRAP_SERVERS_CONFIG}".toString()):"localhost:2222",
                ("kafka.${GROUP_ID_CONFIG}".toString()):"mygroup",
                ("kafka.consumers.default.${MAX_POLL_RECORDS_CONFIG}".toString()):"100",
                ("kafka.consumers.default." + KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka.consumers.default." + VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name
        )

        when:
        KafkaConsumerConfiguration config = applicationContext.getBean(KafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == "localhost:2222"
        props[GROUP_ID_CONFIG] == "mygroup"
        props[MAX_POLL_RECORDS_CONFIG] == "100"

        when:
        Consumer consumer = applicationContext.createBean(Consumer, config)

        then:
        consumer != null

        cleanup:
        consumer.close()
        applicationContext.close()
    }

    void "test configure list fields default properties"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run(
                ("kafka.${BOOTSTRAP_SERVERS_CONFIG}".toString()):Arrays.asList("localhost:1111","localhost:1112"),
                ("kafka.${GROUP_ID_CONFIG}".toString()):"mygroup",
                ("kafka.consumers.default." + KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
                ("kafka.consumers.default." + VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name
        )

        when:
        AbstractKafkaConsumerConfiguration config = applicationContext.getBean(AbstractKafkaConsumerConfiguration)
        Properties props = config.getConfig()

        then:
        props[BOOTSTRAP_SERVERS_CONFIG] == "localhost:1111,localhost:1112"
        props[GROUP_ID_CONFIG] == "mygroup"

        when:
        Consumer consumer = applicationContext.createBean(Consumer, config)

        then:
        consumer != null

        cleanup:
        consumer.close()
        applicationContext.close()
    }
}
