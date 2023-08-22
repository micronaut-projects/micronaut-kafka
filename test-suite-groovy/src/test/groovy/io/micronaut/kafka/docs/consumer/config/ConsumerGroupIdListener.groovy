package io.micronaut.kafka.docs.consumer.config

import io.micronaut.configuration.kafka.annotation.KafkaListener

// tag::annotation[]
@KafkaListener(groupId = 'myGroup')
// end::annotation[]
class ConsumerGroupIdListener {
    // define topic listeners
}
