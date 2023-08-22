package io.micronaut.kafka.docs.seek.rebalance

import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Products
import jakarta.inject.Singleton

@Singleton
@Requires(property = "spec.name", value = "ConsumerRebalanceListenerTest")
class ProductListenerConfiguration constructor(producer: ProductClient) {

    init {
        // Records are produced before ProductListener rebalances
        producer.produce(Products.PRODUCT_0)
        producer.produce(Products.PRODUCT_1)
    }
}
