package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.ConsumerCreationStrategy
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Requires(property = 'spec.name', value = 'ConsumerCreationStrategyStateSpec')
@Singleton
@KafkaListener(batch = true, consumerCreationStrategy = ConsumerCreationStrategy.PER_CLASS)
class TestPerClassMultiTopicBatchListener {
    List<String> fooValues = []
    List<String> barValues = []

    @Topic(['foo', 'foo2'])
    void receiveFoo(List<String> value) {
        fooValues.addAll(value)
    }

    @Topic('bar')
    void receiveBar(List<String> value) {
        barValues.addAll(value)
    }
}
