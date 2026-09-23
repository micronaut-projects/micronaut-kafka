from abc import ABC, abstractmethod

from micronaut.configuration.kafka.annotation import KafkaClient, Topic
from micronaut.context.annotation import Requires
from org.apache.kafka.clients.producer import RecordMetadata
from reactor.core.publisher import Flux

from .Book import Book


@Requires(property="spec.name", value="BookListenerTest")
# tag::clazz[]
@KafkaClient(batch=True)
class BookClient(ABC):
# end::clazz[]

    # tag::lists[]
    @Topic("books")
    @abstractmethod
    def send_list(self, books: list[Book]) -> None:
        ...
    # end::lists[]

    # tag::arrays[]
    # TODO(python): variadic parameters (Book...) are not mapped to Java arrays, pass a list instead
    # end::arrays[]

    # tag::reactive[]
    @Topic("books")
    @abstractmethod
    def send(self, books: list[Book]) -> Flux[RecordMetadata]:
        ...
    # end::reactive[]

    # tag::flux[]
    @Topic("books")
    @abstractmethod
    def send_flux(self, books: Flux[Book]) -> Flux[RecordMetadata]:
        ...
    # end::flux[]
