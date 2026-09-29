from abc import ABC, abstractmethod

from micronaut.configuration.kafka.annotation import KafkaClient, Topic
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="RetryTopicProductListenerTest")
# tag::client[]
@KafkaClient("product-client")
class RetryTopicProductClient(ABC):

    @Topic("products")
    @abstractmethod
    def send_product(self, product: str) -> None:
        ...
# end::client[]
