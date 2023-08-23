package io.micronaut.kafka.docs.consumer.errors;

import io.micronaut.configuration.kafka.annotation.ErrorStrategy;
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.context.annotation.Requires;

@Requires(property = "spec.name", value = "StrategyProductListenerTest")
// tag::annotation[]
@KafkaListener(
    value = "myGroup",
    errorStrategy = @ErrorStrategy(
        value = ErrorStrategyValue.RETRY_ON_ERROR,
        retryDelay = "50ms",
        retryCount = 3
    )
)
// end::annotation[]
public class StrategyProductListener {
}
