package io.micronaut.kafka.docs.consumer.scope

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.KafkaScope
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import jakarta.inject.Inject
import java.util.UUID

// tag::listener[]
@KafkaListener(offsetReset = OffsetReset.EARLIEST)
class ProductListener {

    // tag::scope[]
    @KafkaScope
    open class ProductMetadata {
        private val correlationId: String = UUID.randomUUID().toString()

        open fun correlationId(): String = correlationId
    }
    // end::scope[]

    @Inject lateinit var productMetadata: ProductMetadata

    @Topic("products")
    fun receive(product: String) {
        println("Received $product with correlation ${productMetadata.correlationId()}")
    }
}
// end::listener[]
