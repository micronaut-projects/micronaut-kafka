package io.micronaut.configuration.kafka.docs.consumer.batch;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.List;
// end::imports[]

// tag::clazz[]
@KafkaListener(batch = true) // <1>
public class BookListener {
// end::clazz[]

    // tag::method[]
    @Topic("all-the-books")
    public void receiveList(List<Book> books) { // <2>
        for (Book book : books) {
            System.out.println("Got Book = " + book.getTitle()); // <3>
        }
    }
    // end::method[]

    // tag::reactive[]
    @Topic("all-the-books")
    public Flux<Book> receiveFlux(Flux<Book> books) {
        return books.doOnNext(book ->
                System.out.println("Got Book = " + book.getTitle())
        );
    }
    // end::reactive[]
}
