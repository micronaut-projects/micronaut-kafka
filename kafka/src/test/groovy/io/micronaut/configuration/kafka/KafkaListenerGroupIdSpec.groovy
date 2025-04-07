package io.micronaut.configuration.kafka

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import spock.lang.AutoCleanup
import spock.lang.Specification

class KafkaListenerGroupIdSpec extends Specification {

    @AutoCleanup ApplicationContext applicationContext

    @Requires(property = 'spec.name', value = 'KafkaListenerGroupIdSpec')
    @KafkaListener(id = "WITH_ID_1", groupId = "GROUP_ID_FALLBACK", autoStartup = false)
    static class ConsumerWithIdAndGroupId implements ConsumerAware<String, String> {
        Consumer<String, String> kafkaConsumer
        @Topic("foo") void consume(String foo) { }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerGroupIdSpec')
    @KafkaListener(id = "WITH_ID_2", autoStartup = false)
    static class ConsumerWithId implements ConsumerAware<String, String> {
        Consumer<String, String> kafkaConsumer
        @Topic("foo") void consume(String foo) { }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerGroupIdSpec')
    @KafkaListener(groupId = "GROUP_ID_FALLBACK", autoStartup = false)
    static class ConsumerWithGroupId implements ConsumerAware<String, String> {
        Consumer<String, String> kafkaConsumer
        @Topic("foo") void consume(String foo) { }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerGroupIdSpec')
    @KafkaListener(autoStartup = false)
    static class Without implements ConsumerAware<String, String> {
        Consumer<String, String> kafkaConsumer
        @Topic("foo") void consume(String foo) { }
    }

    void "test named consumers"() {
        given:
        applicationContext = ApplicationContext.run(
                'spec.name': 'KafkaListenerGroupIdSpec',
                ('kafka.' + ConsumerConfig.CLIENT_ID_CONFIG): "CLIENT_0",
                ('kafka.consumers.with-id-1.' + ConsumerConfig.CLIENT_ID_CONFIG): "CLIENT_1",
                ('kafka.consumers.with-id-1.' + ConsumerConfig.GROUP_ID_CONFIG): "GROUP_ID_CONFIG_1",
                ('kafka.consumers.with-id-2.' + ConsumerConfig.CLIENT_ID_CONFIG): "CLIENT_2",
                ('kafka.consumers.with-id-2.' + ConsumerConfig.GROUP_ID_CONFIG): "GROUP_ID_CONFIG_2",
                ('kafka.consumers.group-id-fallback.' + ConsumerConfig.CLIENT_ID_CONFIG): "CLIENT_3",
                ('kafka.consumers.group-id-fallback.' + ConsumerConfig.GROUP_ID_CONFIG): "GROUP_ID_CONFIG_3"
        )

        when:
        ConsumerWithIdAndGroupId withIdAndGroupId = applicationContext.getBean(ConsumerWithIdAndGroupId)

        then:
        withIdAndGroupId != null
        withIdAndGroupId.kafkaConsumer.delegate.clientId == 'CLIENT_1'
        withIdAndGroupId.kafkaConsumer.delegate.groupId.get() == 'GROUP_ID_FALLBACK'


        when:
        ConsumerWithId withId = applicationContext.getBean(ConsumerWithId)

        then:
        withId != null
        withId.kafkaConsumer.delegate.clientId == 'CLIENT_2'
        withId.kafkaConsumer.delegate.groupId.get() == 'GROUP_ID_CONFIG_2'


        when:
        ConsumerWithGroupId withGroupId = applicationContext.getBean(ConsumerWithGroupId)

        then:
        withGroupId != null
        withGroupId.kafkaConsumer.delegate.clientId == 'CLIENT_3'
        withGroupId.kafkaConsumer.delegate.groupId.get() == 'GROUP_ID_FALLBACK'


        when:
        Without withoutIdAndGroupId = applicationContext.getBean(Without)

        then:
        withoutIdAndGroupId != null
        withoutIdAndGroupId.kafkaConsumer.delegate.clientId == 'CLIENT_0'
        withoutIdAndGroupId.kafkaConsumer.delegate.groupId.get() == Without.name

        cleanup:
        applicationContext.close()
    }
}
