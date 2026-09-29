from micronaut.configuration.kafka.annotation import KafkaListener, OffsetReset, OffsetStrategy, Topic
from micronaut.context.annotation import Requires
from micronaut.kafka.docs.consumer.batch.Book import Book
from micronaut.messaging import Acknowledgement


@Requires(property="spec.name", value="BatchManualAckSpec")
class BookListener:

    # tag::method[]
    @KafkaListener(offsetReset=OffsetReset.EARLIEST, offsetStrategy=OffsetStrategy.DISABLED, batch=True)  # <1>
    @Topic("all-the-books")
    def receive(self, books: list[Book], acknowledgement: Acknowledgement) -> None:  # <2>

        # process the books

        acknowledgement.ack()  # <3>
    # end::method[]
