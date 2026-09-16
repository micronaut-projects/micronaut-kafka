from typing import Annotated

from jakarta.inject import Inject
from micronaut.context import ApplicationContext
from micronaut.context.annotation import Property
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.junit.jupiter.api import Test

from .Book import Book
from .BookSender import BookSender


# tag::test[]
@Property(name="spec.name", value="BookSenderTest")
@Property(name="kafka.enabled", value="true")
@MicronautTest(environments=["kafka"])  # <1>
class BookSenderTest:
    ctx: Annotated[ApplicationContext, Inject]

    @Test
    def test_book_sender(self):
        book_sender = self.ctx.getBean(BookSender).asPolyglotValue()  # <2>
        book = Book("The Stand")
        stephen_king = book_sender.send("Stephen King", book)
        record_metadata = stephen_king.get()
        assert record_metadata.topic() == "books"
# end::test[]
