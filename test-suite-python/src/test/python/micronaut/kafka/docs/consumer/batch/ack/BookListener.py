from micronaut.configuration.kafka.annotation import KafkaListener, OffsetReset, OffsetStrategy, Topic
from micronaut.context.annotation import Requires
from micronaut.kafka.docs.consumer.batch.Book import Book
from micronaut.messaging import Acknowledgement


# TODO(python): a method decorated with @KafkaListener is treated as a factory method by the
# Python compiler, so the annotation is declared on the class in Python.
@Requires(property="spec.name", value="BatchManualAckSpec")
# tag::method[]
@KafkaListener(offsetReset=OffsetReset.EARLIEST, offsetStrategy=OffsetStrategy.DISABLED, batch=True)  # <1>
class BookListener:

    @Topic("all-the-books")
    def receive(self, books: list[Book], acknowledgement: Acknowledgement) -> None:  # <2>

        # process the books

        acknowledgement.ack()  # <3>
# end::method[]
