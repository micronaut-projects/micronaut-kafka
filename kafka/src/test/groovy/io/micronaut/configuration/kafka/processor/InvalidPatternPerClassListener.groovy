package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.ConsumerCreationStrategy
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires

@Requires(property = 'spec.name', value = 'ConsumerCreationStrategySupportSpec')
@KafkaListener(consumerCreationStrategy = ConsumerCreationStrategy.PER_CLASS)
class InvalidPatternPerClassListener {
    @Topic(patterns = ['[foo'])
    void receive(String value) {
    }
}
