package io.micronaut.kafka.docs.seek.aware

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product

@Requires(property = "spec.name", value = "ConsumerSeekAwareTest")
@KafkaClient
interface ProductClient {
    @Topic("wonderful-products")
    fun produce(product: Product)
}
