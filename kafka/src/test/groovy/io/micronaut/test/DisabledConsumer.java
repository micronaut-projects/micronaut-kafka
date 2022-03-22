package io.micronaut.test;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Requires(property = "spec.name", value = "DisabledSpec")
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
