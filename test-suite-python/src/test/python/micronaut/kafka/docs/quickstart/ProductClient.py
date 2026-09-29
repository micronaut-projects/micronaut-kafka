from abc import ABC, abstractmethod
from typing import Annotated

# tag::imports[]
from micronaut.configuration.kafka.annotation import KafkaClient, KafkaKey, Topic
from micronaut.context.annotation import Requires
# end::imports[]


@Requires(property="spec.name", value="QuickstartTest")
# tag::clazz[]
@KafkaClient  # <1>
class ProductClient(ABC):

    @Topic("my-products")  # <2>
    @abstractmethod
    def send_product(self, brand: Annotated[str, KafkaKey], name: str) -> None:  # <3>
        ...

    @abstractmethod
    def send_product_to_topic(self, topic: Annotated[str, Topic], brand: Annotated[str, KafkaKey], name: str) -> None:  # <4>
        ...
# end::clazz[]
