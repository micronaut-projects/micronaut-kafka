package io.micronaut.kafka.docs.consumer.offsets.ack

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product
import io.micronaut.messaging.Acknowledgement

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.DISABLED

@Requires(property = 'spec.name', value = 'AckProductListenerTest')
// tag::clazz[]
@KafkaListener(offsetReset = EARLIEST, offsetStrategy = DISABLED) // <1>
class ProductListener {

    @Topic('awesome-products')
    void receive(Product product, Acknowledgement acknowledgement) { // <2>
        // process product record
        acknowledgement.ack() // <3>
    }
}
// end::clazz[]
