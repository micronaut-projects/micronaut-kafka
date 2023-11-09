package io.micronaut.kafka.docs.consumer.batch.ack

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.consumer.batch.Book
import io.micronaut.messaging.Acknowledgement

@Requires(property = "spec.name", value = "BatchManualAckSpec")
internal class BookListener {

    // tag::method[]
    @KafkaListener(offsetReset = OffsetReset.EARLIEST, offsetStrategy = OffsetStrategy.DISABLED, batch = true) // <1>
    @Topic("all-the-books")
    fun receive(books: List<Book?>?, acknowledgement: Acknowledgement) { // <2>

        //process the books

        acknowledgement.ack() // <3>
    }
    // end::method[]
}
