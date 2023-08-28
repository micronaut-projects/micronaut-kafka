package io.micronaut.kafka.docs.consumer.topics

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires

@Requires(property = 'spec.name', value = 'TopicsProductListenerTest')
@Slf4j
@KafkaListener
class ProductListener {

    // tag::multiTopics[]
    @Topic(['fun-products', 'awesome-products'])
    void receiveMultiTopics(@KafkaKey String brand, String name) {
        log.info("Got Product - {} by {}", name, brand)
    }
    // end::multiTopics[]

    // tag::patternTopics[]
    @Topic(patterns='products-\\w+')
    void receivePatternTopics(@KafkaKey String brand, String name) {
        log.info("Got Product - {} by {}", name, brand)
    }
    // end::patternTopics[]
}
