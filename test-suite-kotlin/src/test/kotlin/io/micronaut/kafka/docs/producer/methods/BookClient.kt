package io.micronaut.kafka.docs.producer.methods

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import org.apache.kafka.clients.producer.RecordMetadata
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Requires(property = "spec.name", value = "BookClientTest")
@KafkaClient("product-client")
interface BookClient {

    // tag::mono[]
    @Topic("my-books")
    fun sendBook(@KafkaKey author: String, book: Mono<Book>): Mono<Book>
    // end::mono[]

    // tag::flux[]
    @Topic("my-books")
    fun sendBooks(@KafkaKey author: String, book: Flux<Book>): Flux<RecordMetadata>
    // end::flux[]
}
