package io.micronaut.configuration.kafka.docs.consumer.offsets.ack;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.docs.consumer.config.Product;
import io.micronaut.messaging.Acknowledgement;

class ProductListener {

    // tag::method[]
    @KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        offsetStrategy = OffsetStrategy.DISABLED // <1>
    )
    @Topic("awesome-products")
    void receive(Product product,
                 Acknowledgement acknowledgement) { // <2>

        // process product record

        acknowledgement.ack(); // <3>
    }
    // end::method[]
}
