package io.micronaut.kafka.docs.consumer.batch.ack

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.consumer.batch.Book
import io.micronaut.messaging.Acknowledgement

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.DISABLED

@Requires(property = 'spec.name', value = 'BatchManualAckSpec')
class BookListener {

    // tag::method[]
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = DISABLED, batch = true) // <1>
    @Topic("all-the-books")
    void receive(List<Book> books, Acknowledgement acknowledgement) { // <2>

        //process the books

        acknowledgement.ack() // <3>
    }
    // end::method[]
}
