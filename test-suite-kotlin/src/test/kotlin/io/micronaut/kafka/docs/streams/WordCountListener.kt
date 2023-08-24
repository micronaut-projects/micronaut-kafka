package io.micronaut.kafka.docs.streams

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import java.util.*
import java.util.concurrent.ConcurrentHashMap
// end::imports[]

@Requires(property = "spec.name", value = "WordCountStreamTest")
// tag::clazz[]
@KafkaListener(offsetReset = OffsetReset.EARLIEST, groupId = "WordCountListener")
class WordCountListener {

    private val wordCounts: MutableMap<String, Long> = ConcurrentHashMap()

    @Topic("streams-wordcount-output")
    fun count(@KafkaKey word: String, count: Long) {
        wordCounts[word] = count
    }

    fun getCount(word: String): Long {
        val num = wordCounts[word]
        return num ?: 0
    }

    fun getWordCounts(): Map<String, Long> {
        return Collections.unmodifiableMap(wordCounts)
    }
}
// end::clazz[]
