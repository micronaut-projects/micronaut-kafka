package io.micronaut.kafka.docs.consumer.errors

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.MessageHeaders

@Requires(property = "spec.name", value = "RetryTopicProductListenerTest")
// tag::dlt[]
@KafkaListener("product-retry-dlt-group")
class RetryTopicProductDltListener {

    @Topic("products-dlt")
    fun receive(product: String, headers: MessageHeaders) {
        val originalTopic = headers.get("micronaut-kafka-original-topic", String::class.java).orElse("unknown")
        println("Routing $product from $originalTopic to the dead letter topic")
    }
}
// end::dlt[]
