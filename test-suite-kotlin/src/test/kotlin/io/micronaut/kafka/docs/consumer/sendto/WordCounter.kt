package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.configuration.kafka.KafkaMessage
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.messaging.annotation.SendTo
import org.apache.kafka.common.IsolationLevel

// tag::transactional[]
@KafkaListener(
    producerClientId = "word-counter-producer",
    producerTransactionalId = "tx-word-counter-id",
    offsetStrategy = OffsetStrategy.SEND_TO_TRANSACTION,
    isolation = IsolationLevel.READ_COMMITTED
)
class WordCounter {

    @Topic("tx-incoming-strings")
    @SendTo("my-words-count")
    fun wordsCounter(string: String) = string
        .split(Regex("\\s+"))
        .groupBy { it }
        .map { KafkaMessage.Builder.withBody<String, Int>(it.value.size).key(it.key).build() }
}
