# tag::imports[]
from typing import Annotated

from jakarta.inject import Singleton
from java.util.concurrent import Future
from micronaut.configuration.kafka.annotation import KafkaClient
from micronaut.context.annotation import Requires
from org.apache.kafka.clients.producer import Producer, ProducerRecord, RecordMetadata

from .Book import Book
# end::imports[]


@Requires(property="spec.name", value="BookSenderTest")
# tag::clazz[]
@Singleton
class BookSender:

    def __init__(self, kafka_producer: Annotated[Producer[str, Book], KafkaClient("book-producer")]):  # <1>
        self.kafka_producer = kafka_producer

    def send(self, author: str, book: Book) -> Future[RecordMetadata]:
        return self.kafka_producer.send(ProducerRecord("books", author, book))  # <2>
# end::clazz[]
