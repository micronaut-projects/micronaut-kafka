package io.micronaut.kafka.docs.consumer.batch;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.Flux;

import java.util.List;

@Requires(property = "spec.name", value = "BookListenerTest")
// tag::clazz[]
@KafkaClient(batch = true)
public interface BookClient {
// end::clazz[]

    // tag::lists[]
    @Topic("books")
    void sendList(List<Book> books);
    // end::lists[]

    // tag::arrays[]
    @Topic("books")
    void sendBooks(Book...books);
    // end::arrays[]

    // tag::reactive[]
    @Topic("books")
    Flux<RecordMetadata> send(List<Book> books);
    // end::reactive[]

    // tag::flux[]
    @Topic("books")
    Flux<RecordMetadata> send(Flux<Book> books);
    // end::flux[]
}
