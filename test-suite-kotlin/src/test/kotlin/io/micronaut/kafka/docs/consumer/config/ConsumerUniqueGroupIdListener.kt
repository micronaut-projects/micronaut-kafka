package io.micronaut.kafka.docs.consumer.config

import io.micronaut.configuration.kafka.annotation.KafkaListener

// tag::annotation[]
@KafkaListener(groupId = "myGroup", uniqueGroupId = true)
// end::annotation[]
class ConsumerUniqueGroupIdListener {
    // define topic listeners
}
