package io.micronaut.kafka.docs.consumer.batch

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.reactivex.Flowable
import org.apache.kafka.clients.producer.RecordMetadata

@Requires(property = 'spec.name', value = 'BookListenerTest')
// tag::clazz[]
@KafkaClient(batch = true)
interface BookClient {
// end::clazz[]

    // tag::lists[]
    @Topic('books')
    void sendList(List<Book> books)
    // end::lists[]

    // tag::arrays[]
    @Topic('books')
    void sendBooks(Book...books)
    // end::arrays[]

    // tag::reactive[]
    @Topic('books')
    Flowable<RecordMetadata> send(List<Book> books)
    // end::reactive[]

    // tag::flowable[]
    @Topic('books')
    Flowable<RecordMetadata> send(Flowable<Book> books)
    // end::flowable[]
}
