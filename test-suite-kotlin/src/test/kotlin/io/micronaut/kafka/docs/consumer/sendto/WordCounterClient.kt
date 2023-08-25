package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.configuration.kafka.KafkaMessage
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires

@Requires(property = "spec.name", value = "WordCounterTest")
@KafkaClient("word-counter-producer")
interface WordCounterClient {

    @Topic("tx-incoming-strings")
    fun send(words: String): List<KafkaMessage<String, Int>>
}
