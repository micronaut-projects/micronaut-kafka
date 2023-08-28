package io.micronaut.kafka.docs.consumer.sendto;

import io.micronaut.configuration.kafka.KafkaMessage;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import io.micronaut.messaging.annotation.SendTo;
import org.apache.kafka.common.IsolationLevel;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Requires(property = "spec.name", value = "WordCounterTest")
// tag::transactional[]
@KafkaListener(
    offsetReset = OffsetReset.EARLIEST,
    producerClientId = "word-counter-producer", // <1>
    producerTransactionalId = "tx-word-counter-id", // <2>
    offsetStrategy = OffsetStrategy.SEND_TO_TRANSACTION, // <3>
    isolation = IsolationLevel.READ_COMMITTED // <4>
)
public class WordCounter {

    @Topic("tx-incoming-strings")
    @SendTo("my-words-count")
    List<KafkaMessage<byte[], Integer>> wordsCounter(String string) {
        return Stream.of(string.split("\\s+"))
            .collect(Collectors.groupingBy(Function.identity(), Collectors.summingInt(i -> 1)))
            .entrySet()
            .stream()
            .map(e -> KafkaMessage.Builder.<byte[], Integer>withBody(e.getValue()).key(e.getKey().getBytes()).build())
            .toList();
    }
}
// end::transactional[]
