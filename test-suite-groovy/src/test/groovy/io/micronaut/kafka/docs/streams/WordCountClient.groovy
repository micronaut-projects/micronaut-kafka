package io.micronaut.kafka.docs.streams

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
// end::imports[]

@Requires(property = 'spec.name', value = 'WordCountStreamTest')
// tag::clazz[]
@KafkaClient
interface WordCountClient {

    @Topic("streams-plaintext-input")
    void publishSentence(String sentence)
}
// end::clazz[]
