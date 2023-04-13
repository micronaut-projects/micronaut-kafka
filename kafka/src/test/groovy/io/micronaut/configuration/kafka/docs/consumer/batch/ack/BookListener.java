package io.micronaut.configuration.kafka.docs.consumer.batch.ack;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.docs.consumer.batch.Book;
import io.micronaut.messaging.Acknowledgement;

import java.util.List;

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST;
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.DISABLED;
// end::imports[]

class BookListener {

    // tag::method[]
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = DISABLED, batch = true) // <1>
    @Topic("all-the-books")
    public void receive(List<Book> books,
                        Acknowledgement acknowledgement) { // <2>

        //process the books

        acknowledgement.ack(); // <3>
    }
    // end::method[]
}
