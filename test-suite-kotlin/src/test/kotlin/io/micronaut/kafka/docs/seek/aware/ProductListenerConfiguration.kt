package io.micronaut.kafka.docs.seek.aware

import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Products
import jakarta.inject.Singleton

@Singleton
@Requires(property = "spec.name", value = "ConsumerSeekAwareTest")
class ProductListenerConfiguration constructor(producer: ProductClient) {

    init {
        // Records are produced before ProductListener rebalances
        producer.produce(Products.PRODUCT_0)
        producer.produce(Products.PRODUCT_1)
    }
}
