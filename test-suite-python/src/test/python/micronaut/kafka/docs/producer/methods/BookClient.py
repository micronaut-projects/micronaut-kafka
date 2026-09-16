from abc import ABC, abstractmethod
from typing import Annotated

from micronaut.configuration.kafka.annotation import KafkaClient, KafkaKey, Topic
from micronaut.context.annotation import Requires
from micronaut.scheduling import TaskExecutors
from org.apache.kafka.clients.producer import RecordMetadata
from reactor.core.publisher import Flux, Mono

from .Book import Book


@Requires(property="spec.name", value="BookClientTest")
#tag::clazz[]
@KafkaClient(value="product-client", executor=TaskExecutors.BLOCKING)
class BookClient(ABC):
#end::clazz[]

    #tag::mono[]
    @Topic("my-books")
    @abstractmethod
    def send_book(self, author: Annotated[str, KafkaKey], book: Mono[Book]) -> Mono[Book]:
        ...
    #end::mono[]

    #tag::flux[]
    @Topic("my-books")
    @abstractmethod
    def send_books(self, author: Annotated[str, KafkaKey], book: Flux[Book]) -> Flux[RecordMetadata]:
        ...
    #end::flux[]
