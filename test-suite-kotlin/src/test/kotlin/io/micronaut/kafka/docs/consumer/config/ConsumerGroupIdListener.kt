package io.micronaut.kafka.docs.consumer.config

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.context.annotation.Requires

@Requires(property = "spec.name", value = "ConfigProductListenerTest") // tag::clazz[]
// tag::annotation[]
@KafkaListener(groupId = "myGroup")
// end::annotation[]
class ConsumerGroupIdListener {
    // define topic listeners
}

