package io.micronaut.kafka.docs.streams

import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import io.micronaut.testcontainers.kafka.Kafka
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.apache.kafka.streams.KafkaStreams
import java.util.concurrent.TimeUnit

internal class WordCountStreamTest {

    @Test
    @Suppress("UNCHECKED_CAST")
    fun testWordCounter() {
        val props = Kafka.getProperties() as MutableMap<String, Any>
        props.putAll(
            mapOf(
                "kafka.enabled" to StringUtils.TRUE,
                "spec.name" to "WordCountStreamTest",
                "kafka.streams.my-stream.application.id" to "test-suite-kotlin-my-stream",
                "kafka.streams.my-stream.start-kafka-streams" to StringUtils.FALSE,
                "kafka.streams.my-other-stream.application.id" to "test-suite-kotlin-my-other-stream",
                "kafka.streams.my-other-stream.start-kafka-streams" to StringUtils.FALSE
            )
        )
        ApplicationContext.run(props).use { ctx ->
            await().atMost(30, TimeUnit.SECONDS).until {
                ctx.getBeansOfType(KafkaStreams::class.java).all { stream -> stream.state().isRunningOrRebalancing }
            }

            val client = ctx.getBean(WordCountClient::class.java)
            client.publishSentence("test to test for words")

            val listener = ctx.getBean(WordCountListener::class.java)

            await().atMost(30, TimeUnit.SECONDS).until {
                listener.getWordCounts().size == 4 &&
                        listener.getCount("test") == 2L &&
                        listener.getCount("to") == 1L &&
                        listener.getCount("for") == 1L &&
                        listener.getCount("words") == 1L
            }
        }
    }
}
