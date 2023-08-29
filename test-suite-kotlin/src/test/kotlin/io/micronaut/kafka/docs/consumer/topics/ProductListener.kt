package io.micronaut.kafka.docs.consumer.topics

import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import org.slf4j.LoggerFactory

@Requires(property = "spec.name", value = "TopicsProductListenerTest")
@KafkaListener
class ProductListener {

    companion object {
        private val LOG = LoggerFactory.getLogger(ProductListener::class.java)
    }

    // tag::multiTopics[]
    @Topic("fun-products", "awesome-products")
    fun receiveMultiTopics(@KafkaKey brand: String, name: String) {
        LOG.info("Got Product - {} by {}", name, brand)
    }
    // end::multiTopics[]

    // tag::patternTopics[]
    @Topic(patterns = ["products-\\w+"])
    fun receivePatternTopics(@KafkaKey brand: String, name: String) {
        LOG.info("Got Product - {} by {}", name, brand)
    }
    // end::patternTopics[]
}
