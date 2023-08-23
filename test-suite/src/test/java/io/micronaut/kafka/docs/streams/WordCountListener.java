package io.micronaut.kafka.docs.streams;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST;
// end::imports[]

@Requires(property = "spec.name", value = "WordCountStreamTest")
// tag::clazz[]
@KafkaListener(offsetReset = EARLIEST, groupId = "WordCountListener")
public class WordCountListener {

    private final Map<String, Long> wordCounts = new ConcurrentHashMap<>();

    @Topic(WordCountStream.OUTPUT)
    void count(@KafkaKey String word, long count) {
        wordCounts.put(word, count);
    }

    public long getCount(String word) {
        Long num = wordCounts.get(word);
        return num != null ? num : 0;
    }

    public Map<String, Long> getWordCounts() {
        return Collections.unmodifiableMap(wordCounts);
    }
}
// end::clazz[]
