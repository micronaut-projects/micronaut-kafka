package io.micronaut.kafka.docs.producer.methods

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.reactivex.Flowable
import io.reactivex.Single
import org.apache.kafka.clients.producer.RecordMetadata
import reactor.core.publisher.Flux

@Requires(property = "spec.name", value = "BookClientTest")
@KafkaClient("product-client")
interface BookClient {

    // tag::single[]
    @Topic("my-books")
    fun sendBook(@KafkaKey author: String, book: Single<Book>): Single<Book>
    // end::single[]

    // tag::flowable[]
    @Topic("my-books")
    fun sendBooks(@KafkaKey author: String, book: Flowable<Book>): Flowable<Book>
    // end::flowable[]

    // tag::flux[]
    @Topic("my-books")
    fun sendBooks(@KafkaKey author: String, book: Flux<Book>): Flux<RecordMetadata>
    // end::flux[]
}
