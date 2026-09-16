from micronaut.configuration.kafka.annotation import ErrorStrategy, ErrorStrategyValue, KafkaListener
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="RetryProductListenerTest")
# tag::annotation[]
@KafkaListener(
    value="myGroup",
    errorStrategy=ErrorStrategy(
        value=ErrorStrategyValue.RETRY_ON_ERROR,
        retryCountValue="${my.retry.count}"
    )
)
# end::annotation[]
class RetryProductListener:
    pass
