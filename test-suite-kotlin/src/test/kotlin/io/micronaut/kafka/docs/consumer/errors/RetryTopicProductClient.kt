package io.micronaut.kafka.docs.consumer.errors

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires

@Requires(property = "spec.name", value = "RetryTopicProductListenerTest")
// tag::client[]
@KafkaClient("product-client")
interface RetryTopicProductClient {

    @Topic("products")
    fun sendProduct(product: String)
}
// end::client[]
