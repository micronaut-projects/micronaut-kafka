package io.micronaut.kafka.docs.consumer.config

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.context.annotation.Requires

@Requires(property = 'spec.name', value = 'ConfigProductListenerTest')
// tag::annotation[]
@KafkaListener('myGroup')
// end::annotation[]
class ConsumerGroupListener {
    // define topic listeners
}
