package io.micronaut.kafka.docs.consumer.sendto;

import io.micronaut.configuration.kafka.KafkaMessage;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;

import java.util.List;

@Requires(property = "spec.name", value = "WordCounterTest")
@KafkaClient("word-counter-producer")
public interface WordCounterClient {

    @Topic("tx-incoming-strings")
    List<KafkaMessage<String, Integer>> send(String words);
}
