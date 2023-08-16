package io.micronaut.kafka.docs.consumer.batch

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import reactor.core.publisher.Flux
import java.util.*
// end::imports[]

// tag::clazz[]
@KafkaListener(batch = true) // <1>
class BookListener {
// end::clazz[]

    // tag::method[]
    @Topic("all-the-books")
    fun receiveList(books: List<Book>) { // <2>
        for (book in books) {
            println("Got Book = " + book.title) // <3>
        }
    }
    // end::method[]

    // tag::reactive[]
    @Topic("all-the-books")
    fun receiveFlux(books: Flux<Book>): Flux<Book> {
        return books.doOnNext { book: Book ->
            println(
                "Got Book = " + book.title
            )
        }
    }
    // end::reactive[]

    // tag::manual[]
    @Topic("all-the-books")
    fun receive(records: List<ConsumerRecord<String?, Book?>>, kafkaConsumer: Consumer<*, *>) { // <1>
        for (i in records.indices) {
            val record = records[i] // <2>

            // process the book
            val book = record.value()

            // commit offsets
            val topic = record.topic()
            val partition = record.partition()
            val offset = record.offset() // <3>
            kafkaConsumer.commitSync(
                Collections.singletonMap( // <4>
                    TopicPartition(topic, partition),
                    OffsetAndMetadata(offset + 1, "my metadata")
                )
            )
        }
    }
    // end::manual[]
}
