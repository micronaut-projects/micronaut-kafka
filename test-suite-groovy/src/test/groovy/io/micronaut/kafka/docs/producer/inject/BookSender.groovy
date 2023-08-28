package io.micronaut.kafka.docs.producer.inject;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

import java.util.concurrent.Future
// end::imports[]

@Requires(property = 'spec.name', value = 'BookSenderTest')
// tag::clazz[]
@Singleton
class BookSender {

    private final Producer<String, Book> kafkaProducer

    BookSender(@KafkaClient('book-producer') Producer<String, Book> kafkaProducer) { // <1>
        this.kafkaProducer = kafkaProducer
    }

    Future<RecordMetadata> send(String author, Book book) {
        kafkaProducer.send(new ProducerRecord<>('books', author, book)) // <2>
    }
}
// end::clazz[]
