package io.micronaut.kafka.docs.consumer.config

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product

@Requires(property = "spec.name", value = "ConfigProductListenerTest")
@KafkaClient("product-client")
interface ProductClient {
    @Topic("awesome-products")
    fun send(@KafkaKey brand: String, product: Product)
}
