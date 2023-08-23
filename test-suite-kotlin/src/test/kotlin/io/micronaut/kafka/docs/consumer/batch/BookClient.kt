package io.micronaut.kafka.docs.consumer.batch

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.reactivex.Flowable
import org.apache.kafka.clients.producer.RecordMetadata

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
    fun send(books: List<Book>): Flowable<RecordMetadata>
    // end::reactive[]

    // tag::flowable[]
    @Topic("books")
    fun send(books: Flowable<Book>): Flowable<RecordMetadata>
    // end::flowable[]
}
