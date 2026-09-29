from java.lang import String
from micronaut.configuration.kafka.annotation import KafkaListener, Topic
from micronaut.context.annotation import Requires
from micronaut.messaging import MessageHeaders


@Requires(property="spec.name", value="RetryTopicProductListenerTest")
# tag::dlt[]
@KafkaListener("product-retry-dlt-group")
class RetryTopicProductDltListener:

    @Topic("products-dlt")
    def receive(self, product: str, headers: MessageHeaders) -> None:
        original_topic = headers.get("micronaut-kafka-original-topic", String).orElse("unknown")
        print(f"Routing {product} from {original_topic} to the dead letter topic")
# end::dlt[]
