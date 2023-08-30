package io.micronaut.kafka.docs.consumer.batch

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import org.apache.kafka.clients.producer.RecordMetadata
import reactor.core.publisher.Flux

@Requires(property = "spec.name", value = "BookListenerTest")
// tag::clazz[]
@KafkaClient(batch = true)
interface BookClient {
    // end::clazz[]

    // tag::lists[]
    @Topic("books")
    fun sendList(books: List<Book>)
    // end::lists[]

    // tag::arrays[]
    @Topic("books")
    fun sendBooks(vararg books: Book)
    // end::arrays[]

    // tag::reactive[]
    @Topic("books")
    fun send(books: List<Book>): Flux<RecordMetadata>
    // end::reactive[]

    // tag::flux[]
    @Topic("books")
    fun send(books: Flux<Book>): Flux<RecordMetadata>
    // end::flux[]
}
