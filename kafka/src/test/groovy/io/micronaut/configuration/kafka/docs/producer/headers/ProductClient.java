package io.micronaut.configuration.kafka.docs.producer.headers;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.messaging.annotation.MessageHeader;
// end::imports[]

// tag::clazz[]
@KafkaClient(id="product-client")
@MessageHeader(name = "X-Token", value = "${my.application.token}")
public interface ProductClient {
// end::clazz[]
}
