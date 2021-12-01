package io.micronaut.configuration.kafka.docs.consumer.sendto;

import io.micronaut.configuration.kafka.KafkaMessage;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.messaging.annotation.SendTo;
import org.apache.kafka.common.IsolationLevel;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// tag::transactional[]
@KafkaListener(producerClientId = "word-counter-producer", // <1>
        producerTransactionalId = "tx-word-counter-id", // <2>
        offsetStrategy = OffsetStrategy.SEND_TO_TRANSACTION, // <3>
        isolation = IsolationLevel.READ_COMMITTED // <4>
)
public class WordCounter {

    @Topic("tx-incoming-strings")
    @SendTo("my-words-count")
    List<KafkaMessage> wordsCounter(String string) {
        Map<String, Integer> wordsCount = Stream.of(string.split(" "))
                .map(word -> new AbstractMap.SimpleEntry<>(word, 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::sum));
        List<KafkaMessage> messages = new ArrayList<>();
        for (Map.Entry<String, Integer> e : wordsCount.entrySet()) {
            messages.add(KafkaMessage.Builder.withBody(e.getValue()).key(e.getKey()).build());
        }
        return messages;
    }

}
// end::transactional[]
