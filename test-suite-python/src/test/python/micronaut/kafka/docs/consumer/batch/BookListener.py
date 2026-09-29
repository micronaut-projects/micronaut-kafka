import logging

# tag::imports[]
from micronaut.configuration.kafka.annotation import KafkaListener, Topic
from micronaut.context.annotation import Requires
from reactor.core.publisher import Flux

from .Book import Book
# end::imports[]

LOG = logging.getLogger(__name__)


@Requires(property="spec.name", value="BatchBookListenerTest")
# tag::clazz[]
@KafkaListener(batch=True)  # <1>
class BookListener:
# end::clazz[]

    # tag::method[]
    @Topic("all-the-books")
    def receive_list(self, books: list[Book]) -> None:  # <2>
        for book in books:
            LOG.info("Got Book = %s", book.title)  # <3>
    # end::method[]

    # tag::reactive[]
    @Topic("all-the-books")
    def receive_flux(self, books: Flux[Book]) -> Flux[Book]:
        return books.doOnNext(lambda book: LOG.info("Got Book = %s", book.title))
    # end::reactive[]
#tag::endclazz[]
#end::endclazz[]
