from abc import ABC, abstractmethod
from typing import Annotated

from micronaut.configuration.kafka.annotation import KafkaClient, KafkaKey, Topic
from micronaut.context.annotation import Requires
from micronaut.messaging.annotation import MessageHeader
from org.apache.kafka.common.header import Header, Headers


@Requires(property="spec.name", value="ProductClientTest")
@KafkaClient("product-client")
class ProductClient(ABC):

    # tag::key[]
    @Topic("my-products")
    @abstractmethod
    def send_product(self, brand: Annotated[str, KafkaKey], name: str) -> None:
        ...
    # end::key[]

    # tag::messageheader[]
    @Topic("my-products")
    @abstractmethod
    def send_product_with_header(self, brand: Annotated[str, KafkaKey], name: str, my_header: Annotated[str, MessageHeader("My-Header")]) -> None:
        ...
    # end::messageheader[]

    # tag::collectionheaders[]
    @Topic("my-bicycles")
    @abstractmethod
    def send_bicycle(self, brand: Annotated[str, KafkaKey], model: str, headers: list[Header]) -> None:
        ...
    # end::collectionheaders[]

    # tag::kafkaheaders[]
    @Topic("my-bicycles")
    @abstractmethod
    def send_bicycle_with_headers(self, brand: Annotated[str, KafkaKey], model: str, headers: Headers) -> None:
        ...
    # end::kafkaheaders[]
