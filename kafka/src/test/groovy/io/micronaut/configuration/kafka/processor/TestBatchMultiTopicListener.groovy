package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic

@KafkaListener(batch = true)
class TestBatchMultiTopicListener {
    int invocations
    List<String> values = []

    @Topic(['foo', 'bar'])
    void receive(List<String> value) {
        invocations++
        values.addAll(value)
    }
}
