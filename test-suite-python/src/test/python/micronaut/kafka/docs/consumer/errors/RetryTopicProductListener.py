from java.lang import String
from micronaut.configuration.kafka.annotation import ErrorStrategy, ErrorStrategyValue, KafkaListener, Topic
from micronaut.context.annotation import Requires
from micronaut.messaging import MessageHeaders


@Requires(property="spec.name", value="RetryTopicProductListenerTest")
# tag::listener[]
@KafkaListener(
    value="product-retry-group",
    errorStrategy=ErrorStrategy(
        value=ErrorStrategyValue.RETRY_TOPIC_ON_ERROR,
        retryTopicSuffixes=["-retry-5s", "-retry-30s"],
        retryTopicDelays=["5s", "30s"],
        dlq="products-dlt"
    )
)
class RetryTopicProductListener:

    @Topic("products")
    def receive(self, product: str, headers: MessageHeaders) -> None:
        retry_attempt = headers.get("micronaut-kafka-retry-attempt", String).orElse("initial")
        source_topic = headers.get("micronaut-kafka-original-topic", String).orElse("products")
        print(f"Processing {product} from {source_topic} (attempt={retry_attempt})")
# end::listener[]
