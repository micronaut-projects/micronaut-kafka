from micronaut.configuration.kafka.annotation import ErrorStrategy, ErrorStrategyValue, KafkaListener
from micronaut.configuration.kafka.exceptions import KafkaListenerException
from micronaut.configuration.kafka.retry import ConditionalRetryBehaviourHandler
from micronaut.context.annotation import Requires

ConditionalRetryBehaviour = ConditionalRetryBehaviourHandler.ConditionalRetryBehaviour


@Requires(property="spec.name", value="RetryProductListenerTest")
# tag::annotation[]
@KafkaListener(
    value="myGroup",
    errorStrategy=ErrorStrategy(
        value=ErrorStrategyValue.RETRY_CONDITIONALLY_ON_ERROR
    )
)
class ConditionalRetryListener(ConditionalRetryBehaviourHandler):

    def conditionalRetryBehaviour(self, exception: KafkaListenerException) -> ConditionalRetryBehaviour:
        return ConditionalRetryBehaviour.RETRY if self.should_retry(exception) else ConditionalRetryBehaviour.SKIP

    # ...
# end::annotation[]
    @staticmethod
    def should_retry(exception: KafkaListenerException) -> bool:
        return True
