package io.micronaut.kafka.docs.consumer.threads;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.context.annotation.Requires;

@Requires(property = 'spec.name', value = 'DynamicThreadsListenerTest')
// tag::annotation[]
@KafkaListener(groupId = 'myGroup', threadsValue = '${my.thread.count}')
// end::annotation[]
class DynamicThreadsListener {
    // define API
}
