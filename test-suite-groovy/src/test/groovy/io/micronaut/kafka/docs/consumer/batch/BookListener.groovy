package io.micronaut.kafka.docs.consumer.batch

import groovy.util.logging.Slf4j

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import reactor.core.publisher.Flux
// end::imports[]

@Requires(property = 'spec.name', value = 'BookListenerTest')
// tag::clazz[]
@KafkaListener(batch = true) // <1>
@Slf4j
class BookListener {
// end::clazz[]

    // tag::method[]
    @Topic("all-the-books")
    void receiveList(List<Book> books) { // <2>
        for (Book book : books) {
            log.info("Got Book = {}", book.title) // <3>
        }
    }
    // end::method[]

    // tag::reactive[]
    @Topic("all-the-books")
    Flux<Book> receiveFlux(Flux<Book> books) {
        books.doOnNext(book ->
            log.info("Got Book = {}", book.title)
        )
    }
    // end::reactive[]
//tag::endclazz[]
}
//end::endclazz[]
