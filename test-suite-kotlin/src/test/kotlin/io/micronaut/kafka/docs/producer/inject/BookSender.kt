package io.micronaut.kafka.docs.producer.inject

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.Future
// end::imports[]

// tag::clazz[]
@Requires(property = "spec.name", value = "BookSenderTest")
@Singleton
class BookSender(
    @param:KafkaClient("book-producer") private val kafkaProducer: Producer<String, Book>) { // <1>

    fun send(author: String, book: Book): Future<RecordMetadata> {
        return kafkaProducer.send(ProducerRecord("books", author, book)) // <2>
    }
}
// end::clazz[]
