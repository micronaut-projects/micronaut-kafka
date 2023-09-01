package io.micronaut.kafka.docs.producer.methods;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Requires(property = "spec.name", value = "BookClientTest")
@KafkaClient("product-client")
public interface BookClient {

    //tag::mono[]
    @Topic("my-books")
    Mono<Book> sendBook(@KafkaKey String author, Mono<Book> book);
    //end::mono[]

    //tag::flux[]
    @Topic("my-books")
    Flux<RecordMetadata> sendBooks(@KafkaKey String author, Flux<Book> book);
    //end::flux[]
}
