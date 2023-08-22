package io.micronaut.kafka.docs.consumer.topics

import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires

@Requires(property = "spec.name", value = "TopicsProductListenerTest")
@KafkaListener
class ProductListener {

    // tag::multiTopics[]
    @Topic("fun-products", "awesome-products")
    fun receiveMultiTopics(@KafkaKey brand: String, name: String) {
        println("Got Product - $name by $brand")
    }
    // tag::multiTopics[]

    // tag::patternTopics[]
    @Topic(patterns = ["products-\\w+"])
    fun receivePatternTopics(@KafkaKey brand: String, name: String) {
        println("Got Product - $name by $brand")
    }
    // tag::patternTopics[]
}
