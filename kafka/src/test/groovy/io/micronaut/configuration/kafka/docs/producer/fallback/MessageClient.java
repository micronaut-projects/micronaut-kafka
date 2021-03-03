package io.micronaut.configuration.kafka.docs.producer.fallback;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaClient
public interface MessageClient {

    @Topic("messages")
    void send(String message);
}
