package io.micronaut.configuration.kafka

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.FloatSerializer
import org.apache.kafka.common.serialization.IntegerSerializer

class KafkaProducerFactorySpec extends AbstractKafkaContainerSpec {

    protected Map<String, Object> getConfiguration() {
        super.configuration + [
                'kafka.producers.my-kebab-id.client.id'       : 'my-kebab-id',
                'kafka.producers.my-kebab-id.key.serializer'  : IntegerSerializer.name,
                'kafka.producers.my-kebab-id.value.serializer': FloatSerializer.name
        ]
    }

    void "test producer with camel-case group id"() {
        given:
        MyService service = context.getBean(MyService)

        expect:
        service != null
        service.kafkaProducer != null
        service.kafkaProducer.producerConfig.getClass('key.serializer') == IntegerSerializer
        service.kafkaProducer.producerConfig.getClass('value.serializer') == FloatSerializer

        cleanup:
        context.close()
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerFactorySpec')
    @Singleton
    static class MyService {
        KafkaProducer kafkaProducer

        MyService(@KafkaClient(id = 'MY_KEBAB_ID') Producer<String, String> kafkaProducer) {
            this.kafkaProducer = kafkaProducer
        }
    }
}
