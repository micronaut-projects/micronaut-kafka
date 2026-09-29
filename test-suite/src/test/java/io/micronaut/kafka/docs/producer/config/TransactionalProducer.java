package io.micronaut.kafka.docs.producer.config;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Requires(property = "spec.name", value = "TransactionalProducerTest")
// tag::clazz[]
@Singleton
public class TransactionalProducer {

    private final Producer<String, String> producer;

    public TransactionalProducer(@KafkaClient(id = "my-client", transactionalId = "my-tx-id") Producer<String, String> producer) {
        this.producer = producer; // <1>
    }

    public void send(String message) {
        try {
            producer.beginTransaction(); // <2>
            producer.send(new ProducerRecord<>("messages", message));
            producer.commitTransaction(); // <3>
        } catch (Exception e) {
            producer.abortTransaction(); // <4>
        }
    }
}
// end::clazz[]
