package io.micronaut.configuration.kafka.processor

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires

@Requires(property = 'spec.name', value = 'ConsumerCreationStrategySupportSpec')
@KafkaListener
@Topic('bean-topic')
class MethodTopicOverridesClassTopicListener {
    @Topic('method-topic')
    void receive(String value) {
    }
}
