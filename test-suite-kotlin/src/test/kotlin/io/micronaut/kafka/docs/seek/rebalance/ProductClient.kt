package io.micronaut.kafka.docs.seek.rebalance

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product

@Requires(property = "spec.name", value = "ConsumerRebalanceListenerTest")
@KafkaClient
interface ProductClient {
    @Topic("awesome-products")
    fun produce(product: Product)
}
