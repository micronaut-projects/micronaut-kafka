package io.micronaut.kafka.docs.producer.methods;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import io.reactivex.Flowable;
import io.reactivex.Single;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.Flux;

@Requires(property = "spec.name", value = "BookClientTest")
@KafkaClient("product-client")
public interface BookClient {

    // tag::single[]
    @Topic("my-books")
    Single<Book> sendBook(@KafkaKey String author, Single<Book> book);
    // end::single[]

    // tag::flowable[]
    @Topic("my-books")
    Flowable<Book> sendBooks(@KafkaKey String author, Flowable<Book> book);
    // end::flowable[]

    // tag::flux[]
    @Topic("my-books")
    Flux<RecordMetadata> sendBooks(@KafkaKey String author, Flux<Book> book);
    // end::flux[]
}
