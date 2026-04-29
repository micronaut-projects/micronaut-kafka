package io.micronaut.configuration.kafka

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import spock.lang.AutoCleanup
import spock.lang.Specification

class KafkaListenerIdSpec extends Specification {

    @AutoCleanup
    ApplicationContext applicationContext

    void "test listener id controls config lookup and group id fallback"() {
        given:
        applicationContext = ApplicationContext.run(
            'spec.name': 'KafkaListenerIdSpec',
            'micronaut.application.name': 'test-app',
            ('kafka.' + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG): 'localhost:1111',
            ('kafka.consumers.default.' + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
            ('kafka.consumers.default.' + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG): StringDeserializer.name,
            ('kafka.consumers.with-id-and-group.' + ConsumerConfig.GROUP_ID_CONFIG): 'CONFIGURED_GROUP_1',
            ('kafka.consumers.with-id.' + ConsumerConfig.GROUP_ID_CONFIG): 'CONFIGURED_GROUP_2',
            ('kafka.consumers.group-id-only.' + ConsumerConfig.GROUP_ID_CONFIG): 'CONFIGURED_GROUP_3',
            ('kafka.consumers.test-app.' + ConsumerConfig.GROUP_ID_CONFIG): 'CONFIGURED_GROUP_4'
        )

        expect:
        applicationContext.getBean(ConsumerWithIdAndGroupId).kafkaConsumer.groupMetadata().groupId() == 'ANNOTATION_GROUP'
        applicationContext.getBean(ConsumerWithId).kafkaConsumer.groupMetadata().groupId() == 'CONFIGURED_GROUP_2'
        applicationContext.getBean(ConsumerWithIdFallbackGroupId).kafkaConsumer.groupMetadata().groupId() == 'ID_FALLBACK_GROUP'
        applicationContext.getBean(ConsumerWithGroupId).kafkaConsumer.groupMetadata().groupId() == 'GROUP_ID_ONLY'
        applicationContext.getBean(ConsumerWithFallbackGroupId).kafkaConsumer.groupMetadata().groupId() == 'CONFIGURED_GROUP_4'
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerIdSpec')
    @KafkaListener(id = 'WITH_ID_AND_GROUP', groupId = 'ANNOTATION_GROUP', autoStartup = false)
    static class ConsumerWithIdAndGroupId implements ConsumerAware<String, String> {
        Consumer<String, String> kafkaConsumer

        @Topic('foo')
        void consume(String value) {
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerIdSpec')
    @KafkaListener(id = 'WITH_ID', autoStartup = false)
    static class ConsumerWithId implements ConsumerAware<String, String> {
        Consumer<String, String> kafkaConsumer

        @Topic('foo')
        void consume(String value) {
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerIdSpec')
    @KafkaListener(id = 'ID_FALLBACK_GROUP', autoStartup = false)
    static class ConsumerWithIdFallbackGroupId implements ConsumerAware<String, String> {
        Consumer<String, String> kafkaConsumer

        @Topic('foo')
        void consume(String value) {
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerIdSpec')
    @KafkaListener(groupId = 'GROUP_ID_ONLY', autoStartup = false)
    static class ConsumerWithGroupId implements ConsumerAware<String, String> {
        Consumer<String, String> kafkaConsumer

        @Topic('foo')
        void consume(String value) {
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerIdSpec')
    @KafkaListener(autoStartup = false)
    static class ConsumerWithFallbackGroupId implements ConsumerAware<String, String> {
        Consumer<String, String> kafkaConsumer

        @Topic('foo')
        void consume(String value) {
        }
    }
}
