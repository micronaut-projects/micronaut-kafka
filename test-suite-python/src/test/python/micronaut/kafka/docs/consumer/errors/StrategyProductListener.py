from micronaut.configuration.kafka.annotation import ErrorStrategy, ErrorStrategyValue, KafkaListener
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="StrategyProductListenerTest")
# tag::annotation[]
@KafkaListener(
    value="myGroup",
    errorStrategy=ErrorStrategy(
        value=ErrorStrategyValue.RETRY_ON_ERROR,
        retryDelay="50ms",
        retryCount=3
    )
)
# end::annotation[]
class StrategyProductListener:
    pass
