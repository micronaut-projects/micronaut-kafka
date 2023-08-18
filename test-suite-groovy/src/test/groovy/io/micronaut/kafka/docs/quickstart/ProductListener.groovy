package io.micronaut.kafka.docs.quickstart

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.annotation.KafkaKey

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
// end::imports[]

@Requires(property = 'spec.name', value = 'QuickStartTest')
// tag::clazz[]
@Slf4j
@KafkaListener(offsetReset = OffsetReset.EARLIEST) // <1>
class ProductListener {

    @Topic('my-products') // <2>
    void receive(@KafkaKey String brand, String name) { // <3>
        log.info("Got Product - {} by {}", name, brand)
    }
}
// end::clazz[]
