package io.micronaut.configuration.kafka.config

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.FloatSerializer
import org.apache.kafka.common.serialization.IntegerSerializer
import reactor.core.publisher.Mono
import spock.lang.AutoCleanup
import spock.lang.Specification

class KafkaProducerConfigurationSpec extends Specification {

    @AutoCleanup
    ApplicationContext applicationContext

    void "test producer with camel-case group id"() {
        given:
        final properties = [
                'spec.name'                                   : 'KafkaProducerConfigurationSpec',
                'kafka.bootstrap.servers'                     : 'localhost:1111',
                'kafka.producers.my-kebab-id.client.id'       : 'my-kebab-id',
                'kafka.producers.my-kebab-id.key.serializer'  : IntegerSerializer.name,
                'kafka.producers.my-kebab-id.value.serializer': FloatSerializer.name
        ]
        applicationContext = ApplicationContext.run(properties)

        when:
        MyProducer producer = applicationContext.getBean(MyProducer)

        then:
        producer != null

        when:
        producer.produce('foo')
        MyListener listener = applicationContext.getBean(MyListener)

        then:
        listener != null
        listener.kafkaProducer != null
        listener.kafkaProducer.producerConfig.getClass('key.serializer') == IntegerSerializer
        listener.kafkaProducer.producerConfig.getClass('value.serializer') == FloatSerializer

        cleanup:
        applicationContext.close()
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerConfigurationSpec')
    @KafkaClient(id = 'MY_KEBAB_ID')
    static interface MyProducer {
        @Topic('foo')
        Mono<String> produce(String foo)
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerConfigurationSpec')
    @Singleton
    static class MyListener implements BeanCreatedEventListener<Producer> {
        KafkaProducer kafkaProducer

        @Override
        Producer onCreated(BeanCreatedEvent<Producer> event) {
            if (event.bean instanceof KafkaProducer) {
                final producer = ((KafkaProducer) event.bean)
                if (producer.clientId == 'my-kebab-id') {
                    kafkaProducer = event.bean
                }
            }
            return event.bean
        }
    }
}
