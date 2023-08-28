package io.micronaut.kafka.docs.seek.ops

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product

@Requires(property = "spec.name", value = "KafkaSeekOperationsSpec")
@KafkaClient
interface ProductClient {
    @Topic("amazing-products")
    void produce(Product product)
}
