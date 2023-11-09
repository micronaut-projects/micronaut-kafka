package io.micronaut.kafka.docs.consumer.batch

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import org.slf4j.LoggerFactory.getLogger
import reactor.core.publisher.Flux
import java.util.*
// end::imports[]

@Requires(property = "spec.name", value = "BookListenerTest")
// tag::clazz[]
@KafkaListener(batch = true) // <1>
class BookListener {
// end::clazz[]
    companion object {
        private val LOG = getLogger(BookListener::class.java)
    }

    // tag::method[]
    @Topic("all-the-books")
    fun receiveList(books: List<Book>) { // <2>
        for (book in books) {
            LOG.info("Got Book = {}", book.title) // <3>
        }
    }
    // end::method[]

    // tag::reactive[]
    @Topic("all-the-books")
    fun receiveFlux(books: Flux<Book>): Flux<Book> {
        return books.doOnNext { book: Book ->
            LOG.info("Got Book = {}", book.title)
        }
    }
    // end::reactive[]
//tag::endclazz[]
}
//end::endclazz[]
