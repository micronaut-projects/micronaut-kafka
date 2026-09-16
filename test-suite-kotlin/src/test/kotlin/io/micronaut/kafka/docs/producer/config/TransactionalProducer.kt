package io.micronaut.kafka.docs.producer.config

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord

@Requires(property = "spec.name", value = "TransactionalProducerTest")
// tag::clazz[]
@Singleton
class TransactionalProducer(
    @KafkaClient(id = "my-client", transactionalId = "my-tx-id") private val producer: Producer<String, String> // <1>
) {

    fun send(message: String) {
        try {
            producer.beginTransaction() // <2>
            producer.send(ProducerRecord("messages", message))
            producer.commitTransaction() // <3>
        } catch (e: Exception) {
            producer.abortTransaction() // <4>
        }
    }
}
// end::clazz[]
