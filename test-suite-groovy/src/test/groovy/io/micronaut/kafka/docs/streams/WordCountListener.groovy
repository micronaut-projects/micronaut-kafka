package io.micronaut.kafka.docs.streams

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires

import java.util.concurrent.ConcurrentHashMap

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
// end::imports[]

@Requires(property = 'spec.name', value = 'WordCountStreamTest')
// tag::clazz[]
@KafkaListener(offsetReset = EARLIEST, groupId = 'WordCountListener')
class WordCountListener {

    private final Map<String, Long> wordCounts = new ConcurrentHashMap<>()

    @Topic("streams-wordcount-output")
    void count(@KafkaKey String word, long count) {
        wordCounts.put(word, count)
    }

    long getCount(String word) {
        Long num = wordCounts.get(word)
        num ?: 0
    }

    Map<String, Long> getWordCounts() {
        Collections.unmodifiableMap(wordCounts)
    }
}
// end::clazz[]
