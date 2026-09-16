from abc import ABC, abstractmethod

from micronaut.configuration.kafka.annotation import KafkaClient, Topic
from micronaut.context.annotation import Requires
from micronaut.kafka.docs.Product import Product


@Requires(property="spec.name", value="ConsumerSeekAwareTest")
@KafkaClient
class ProductClient(ABC):

    @Topic("wonderful-products")
    @abstractmethod
    def produce(self, product: Product) -> None:
        ...
