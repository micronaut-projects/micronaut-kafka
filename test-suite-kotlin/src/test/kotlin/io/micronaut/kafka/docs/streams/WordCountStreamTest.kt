package io.micronaut.kafka.docs.streams

import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import io.micronaut.testcontainers.kafka.Kafka
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.State
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.UUID

internal class WordCountStreamTest {

    @Test
    @Suppress("UNCHECKED_CAST")
    fun testWordCounter() {
        val props = Kafka.getProperties() as MutableMap<String, Any>
        val uniqueSuffix = UUID.randomUUID().toString()
        val stateDir = Files.createTempDirectory("test-suite-kotlin-word-count-stream-").toFile().apply {
            deleteOnExit()
        }
        props.putAll(
            mapOf(
                "kafka.enabled" to StringUtils.TRUE,
                "micronaut.application.name" to "test-suite-kotlin-word-count-stream",
                "kafka.streams.default.application.id" to "test-suite-kotlin-word-count-stream-$uniqueSuffix",
                "kafka.streams.default.state.dir" to stateDir.absolutePath,
                "spec.name" to "WordCountStreamTest",
                "kafka.streams.my-stream.application.id" to "test-suite-kotlin-my-stream-$uniqueSuffix",
                "kafka.streams.my-stream.start-kafka-streams" to StringUtils.FALSE,
                "kafka.streams.my-other-stream.application.id" to "test-suite-kotlin-my-other-stream-$uniqueSuffix",
                "kafka.streams.my-other-stream.start-kafka-streams" to StringUtils.FALSE
            )
        )
        ApplicationContext.run(props).use { ctx ->
            await().atMost(30, TimeUnit.SECONDS).until {
                val states = ctx.getBeansOfType(KafkaStreams::class.java).map(KafkaStreams::state)
                states.size == 3 &&
                        states.count(State::isRunningOrRebalancing) == 1 &&
                        states.count { state -> state == State.CREATED } == 2
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
