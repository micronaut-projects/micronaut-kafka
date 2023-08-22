package io.micronaut.kafka.docs.consumer.errors

import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.context.annotation.Requires

@Requires(property = "spec.name", value = "RetryProductListenerTest")
// tag::annotation[]
@KafkaListener(
    value = "myGroup",
    errorStrategy = ErrorStrategy(
        value = ErrorStrategyValue.RETRY_ON_ERROR,
        retryCountValue = "\${my.retry.count}"
    )
)
// end::annotation[]
class RetryProductListener
