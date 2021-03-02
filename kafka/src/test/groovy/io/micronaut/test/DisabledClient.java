package io.micronaut.test;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaClient
public interface DisabledClient {
    @Topic("disabled-topic")
    void send(String message);
}
