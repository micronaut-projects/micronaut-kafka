package io.micronaut.kafka.docs.consumer.config

import groovy.util.logging.Slf4j

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
// end::imports[]

@Requires(property = 'spec.name', value = 'ConfigProductListenerTest')
@Slf4j
// tag::clazz[]
@KafkaListener(
        groupId = 'products',
        pollTimeout = '500ms',
        properties = @Property(name = ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, value = '10000')
)
class ProductListener {
// end::clazz[]

    // tag::method[]
    @Topic('awesome-products')
    void receive(@KafkaKey String brand,  // <1>
                 Product product, // <2>
                long offset, // <3>
                int partition, // <4>
                String topic, // <5>
                long timestamp) { // <6>
        log.info("Got Product - {} by {}", product.name, brand)
    }
    // end::method[]

    // tag::consumeRecord[]
    @Topic("awesome-products")
    public void receive(ConsumerRecord<String, Product> record) { // <1>
        Product product = record.value() // <2>
        String brand = record.key() // <3>
        log.info("Got Product - {} by {}", product.name, brand)
    }
    // end::consumeRecord[]
}
