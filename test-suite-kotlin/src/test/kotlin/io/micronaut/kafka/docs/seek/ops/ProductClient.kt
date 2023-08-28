package io.micronaut.kafka.docs.seek.ops

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product

@Requires(property = "spec.name", value = "KafkaSeekOperationsTest")
@KafkaClient
interface ProductClient {
    @Topic("amazing-products")
    fun produce(product: Product)
}
