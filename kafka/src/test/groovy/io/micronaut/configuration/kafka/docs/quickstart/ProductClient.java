package io.micronaut.configuration.kafka.docs.quickstart;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
// end::imports[]

// tag::clazz[]
@KafkaClient // <1>
public interface ProductClient {

    @Topic("my-products") // <2>
    void sendProduct(@KafkaKey String brand, String name); // <3>

    void sendProduct(@Topic String topic, @KafkaKey String brand, String name); // <4>
}
// end::clazz[]
