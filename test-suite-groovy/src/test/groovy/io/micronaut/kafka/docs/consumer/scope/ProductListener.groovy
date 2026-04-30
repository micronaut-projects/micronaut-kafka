package io.micronaut.kafka.docs.consumer.scope

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.KafkaScope
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import jakarta.inject.Inject

// tag::listener[]
@KafkaListener(offsetReset = OffsetReset.EARLIEST)
class ProductListener {

    // tag::scope[]
    @KafkaScope
    static class ProductMetadata {
        final String correlationId = UUID.randomUUID().toString()
    }
    // end::scope[]

    @Inject ProductMetadata productMetadata

    @Topic('products')
    void receive(String product) {
        println "Received ${product} with correlation ${productMetadata.correlationId}"
    }
}
// end::listener[]
