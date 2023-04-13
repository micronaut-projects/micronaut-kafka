package io.micronaut.configuration.kafka.docs.consumer.offsets.ack;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.docs.consumer.config.Product;
import io.micronaut.messaging.Acknowledgement;
// end::imports[]

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST;
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.DISABLED;

class ProductListener {

    // tag::method[]
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = DISABLED) // <1>
    @Topic("awesome-products")
    void receive(Product product,
                 Acknowledgement acknowledgement) { // <2>

        // process product record

        acknowledgement.ack(); // <3>
    }
    // end::method[]
}
