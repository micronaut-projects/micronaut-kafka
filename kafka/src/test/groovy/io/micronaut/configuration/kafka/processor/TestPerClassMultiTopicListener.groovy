package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.ConsumerCreationStrategy
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Requires(property = 'spec.name', value = 'ConsumerCreationStrategyStateSpec')
@Singleton
@KafkaListener(consumerCreationStrategy = ConsumerCreationStrategy.PER_CLASS)
class TestPerClassMultiTopicListener {
    List<String> fooValues = []
    List<String> barValues = []

    @Topic(['foo', 'foo2'])
    void receiveFoo(String value) {
        fooValues << value
    }

    @Topic('bar')
    void receiveBar(String value) {
        barValues << value
    }
}
