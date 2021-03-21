package io.micronaut.configuration.kafka.docs.producer.inject;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.docs.consumer.batch.Book;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.inject.Singleton;
import java.util.concurrent.Future;
// end::imports[]

// tag::clazz[]
@Singleton
public class BookSender {

    private final Producer<String, Book> kafkaProducer;

    public BookSender(
            @KafkaClient("book-producer") Producer<String, Book> kafkaProducer) { // <1>
        this.kafkaProducer = kafkaProducer;
    }

    public Future<RecordMetadata> send(String author, Book book) {
        return kafkaProducer.send(new ProducerRecord<>("books", author, book)); // <2>
    }

}
// end::clazz[]
