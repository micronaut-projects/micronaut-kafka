package io.micronaut.kafka.docs.consumer.errors

import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.MessageHeaders

@Requires(property = 'spec.name', value = 'RetryTopicProductListenerTest')
// tag::listener[]
@KafkaListener(
    value = 'product-retry-group',
    errorStrategy = @ErrorStrategy(
        value = ErrorStrategyValue.RETRY_TOPIC_ON_ERROR,
        retryTopicSuffixes = ['-retry-5s', '-retry-30s'],
        retryTopicDelays = ['5s', '30s'],
        dlq = 'products-dlt'
    )
)
class RetryTopicProductListener {

    @Topic('products')
    void receive(String product, MessageHeaders headers) {
        String retryAttempt = headers.get('micronaut-kafka-retry-attempt', String).orElse('initial')
        String sourceTopic = headers.get('micronaut-kafka-original-topic', String).orElse('products')
        System.out.printf('Processing %s from %s (attempt=%s)%n', product, sourceTopic, retryAttempt)
    }
}
// end::listener[]
