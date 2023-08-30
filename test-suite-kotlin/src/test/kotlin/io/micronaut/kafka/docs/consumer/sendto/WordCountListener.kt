package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import org.slf4j.LoggerFactory

@Requires(property = "spec.name", value = "WordCounterTest")
@KafkaListener(offsetReset = OffsetReset.EARLIEST)
class WordCountListener {

    companion object {
        private val LOG = LoggerFactory.getLogger(WordCountListener::class.java)
    }

    var wordCount: MutableMap<String, Int> = HashMap()

    @Topic("my-words-count")
    fun receive(@KafkaKey key: ByteArray?, value: Any) {
        val word = String(key!!)
        val count = value as Int
        LOG.info("Got word count - {}: {}", word, count)
        wordCount[word] = count
    }
}
