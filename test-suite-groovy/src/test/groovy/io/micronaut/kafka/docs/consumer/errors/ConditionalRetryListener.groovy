package io.micronaut.kafka.docs.consumer.errors

import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.retry.ConditionalRetryBehaviourHandler
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.context.annotation.Requires

@Requires(property = 'spec.name', value = 'RetryProductListenerTest')
// tag::annotation[]
@KafkaListener(
    value = 'myGroup',
    errorStrategy = @ErrorStrategy(
            value = ErrorStrategyValue.RETRY_CONDITIONALLY_ON_ERROR
    )
)
class ConditionalRetryListener implements ConditionalRetryBehaviourHandler {
    @Override
    ConditionalRetryBehaviour conditionalRetryBehaviour(KafkaListenerException exception) {
        return shouldRetry(exception) ? ConditionalRetryBehaviour.RETRY : ConditionalRetryBehaviour.SKIP
    }

    // ...

// end::annotation[]

    private static boolean shouldRetry(KafkaListenerException exception) {
        return true;
    }
}

