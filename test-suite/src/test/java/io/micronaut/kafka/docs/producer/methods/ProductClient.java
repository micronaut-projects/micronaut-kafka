package io.micronaut.kafka.docs.producer.methods;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import io.micronaut.messaging.annotation.MessageHeader;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.Collection;

@Requires(property = "spec.name", value = "ProductClientTest")
@KafkaClient("product-client")
public interface ProductClient {

    // tag::key[]
    @Topic("my-products")
    void sendProduct(@KafkaKey String brand, String name);
    // end::key[]

    // tag::messageheader[]
    @Topic("my-products")
    void sendProduct(@KafkaKey String brand, String name, @MessageHeader("My-Header") String myHeader);
    // end::messageheader[]

    // tag::collectionheaders[]
    @Topic("my-bicycles")
    void sendBicycle(@KafkaKey String brand, String model, Collection<Header> headers);
    // end::collectionheaders[]

    // tag::kafkaheaders[]
    @Topic("my-bicycles")
    void sendBicycle(@KafkaKey String brand, String model, Headers headers);
    // end::kafkaheaders[]
}
