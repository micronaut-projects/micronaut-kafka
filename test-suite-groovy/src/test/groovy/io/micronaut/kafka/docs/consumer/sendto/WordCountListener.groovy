package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import org.slf4j.Logger

import static org.slf4j.LoggerFactory.getLogger

@Requires(property = 'spec.name', value = 'WordCounterTest')
@KafkaListener(offsetReset = OffsetReset.EARLIEST)
class WordCountListener {

    private static final Logger LOG = getLogger(WordCountListener.class)

    Map<String, Integer> wordCount = [:]

    @Topic("my-words-count")
    void receive(@KafkaKey byte[] key, Object value) {
        final String word = new String(key)
        final Integer count = (Integer) value
        LOG.info("Got word count - {}: {}", word, count)
        this.wordCount.put(word, count)
    }
}
