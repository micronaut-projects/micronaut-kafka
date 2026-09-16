from abc import ABC, abstractmethod
from typing import Annotated

from micronaut.configuration.kafka.annotation import KafkaClient, KafkaKey, Topic
from micronaut.context.annotation import Requires
from micronaut.kafka.docs.Product import Product


@Requires(property="spec.name", value="SendToProductListenerTest")
@KafkaClient("product-client")
class ProductClient(ABC):

    @Topic("sendto-products")
    @abstractmethod
    def send(self, brand: Annotated[str, KafkaKey], product: Product) -> None:
        ...
