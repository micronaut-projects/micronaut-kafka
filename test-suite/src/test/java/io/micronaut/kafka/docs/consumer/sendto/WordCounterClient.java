package io.micronaut.kafka.docs.consumer.sendto;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;

@Requires(property = "spec.name", value = "WordCounterTest")
@KafkaClient("word-counter-producer")
public interface WordCounterClient {

    @Topic("tx-incoming-strings")
    void send(String words);
}
