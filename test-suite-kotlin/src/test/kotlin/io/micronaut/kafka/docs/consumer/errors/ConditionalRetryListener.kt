package io.micronaut.kafka.docs.consumer.errors

import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.retry.ConditionalRetryBehaviourHandler
import io.micronaut.configuration.kafka.retry.ConditionalRetryBehaviourHandler.ConditionalRetryBehaviour
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.context.annotation.Requires

@Requires(property = "spec.name", value = "RetryProductListenerTest")
// tag::annotation[]
@KafkaListener(
    value = "myGroup",
    errorStrategy = ErrorStrategy(
        value = ErrorStrategyValue.RETRY_CONDITIONALLY_ON_ERROR
    )
)
class ConditionalRetryListener :
    ConditionalRetryBehaviourHandler {
    override fun conditionalRetryBehaviour(exception: KafkaListenerException): ConditionalRetryBehaviour {
        return if (shouldRetry(exception)) {
            ConditionalRetryBehaviour.RETRY
        } else {
            ConditionalRetryBehaviour.SKIP
        }
    }

    // ...

    // end::annotation[]
    companion object {
        private fun shouldRetry(exception: KafkaListenerException): Boolean {
            return true
        }
    }
}


