package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.configuration.kafka.KafkaMessage
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.messaging.annotation.SendTo
import org.apache.kafka.common.IsolationLevel

// tag::transactional[]
@KafkaListener(
        producerClientId = 'word-counter-producer', // <1>
        producerTransactionalId = 'tx-word-counter-id', // <2>
        offsetStrategy = OffsetStrategy.SEND_TO_TRANSACTION, // <3>
        isolation = IsolationLevel.READ_COMMITTED // <4>
)
class WordCounter {

    @Topic('tx-incoming-strings')
    @SendTo('my-words-count')
    List<KafkaMessage<String, Integer>> wordsCounter(String string) {
        string.split("\\s+")
            .groupBy()
            .collect { key, instanceList ->
                KafkaMessage.Builder.withBody(instanceList.size()).key(key).build()
            }
    }
}
// end::transactional[]
