package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.configuration.kafka.KafkaMessage
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.SendTo
import org.apache.kafka.common.IsolationLevel

@Requires(property = "spec.name", value = "WordCounterTest")
// tag::transactional[]
@KafkaListener(
    offsetReset = OffsetReset.EARLIEST,
    producerClientId = "word-counter-producer", // <1>
    producerTransactionalId = "tx-word-counter-id", // <2>
    offsetStrategy = OffsetStrategy.SEND_TO_TRANSACTION, // <3>
    isolation = IsolationLevel.READ_COMMITTED // <4>
)
class WordCounter {

    @Topic("tx-incoming-strings")
    @SendTo("my-words-count")
    fun wordsCounter(string: String) = string
        .split(Regex("\\s+"))
        .groupBy { it }
        .map { KafkaMessage.Builder.withBody<ByteArray, Int>(it.value.size).key(it.key.toByteArray()).build() }
}
// end::transactional[]
