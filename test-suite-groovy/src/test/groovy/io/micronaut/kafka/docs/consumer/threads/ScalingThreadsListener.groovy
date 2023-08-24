package io.micronaut.kafka.docs.consumer.threads

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.context.annotation.Requires

@Requires(property = 'spec.name', value = 'ScalingThreadsListenerTest')
// tag::annotation[]
@KafkaListener(groupId='myGroup', threads = 10)
// end::annotation[]
class ScalingThreadsListener {
    // define API
}
