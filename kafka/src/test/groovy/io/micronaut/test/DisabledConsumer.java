package io.micronaut.test;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;

import jakarta.inject.Singleton;

@Singleton
public class DisabledConsumer {

    @KafkaListener
    @Topic("disable-topic")
    void receive(@KafkaKey String key, String value) {
        throw new UnsupportedOperationException();
    }

    int getNum() {
        return 1;
    }
}
