package io.micronaut.kafka.docs.streams

import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import org.awaitility.Awaitility
import org.junit.jupiter.api.Test
import java.util.Map
import java.util.concurrent.TimeUnit

internal class WordCountStreamTest {

    @Test
    fun testWordCounter() {
        ApplicationContext.run(
            Map.of<String, Any>("kafka.enabled", StringUtils.TRUE, "spec.name", "WordCountStreamTest")
        ).use { ctx ->
            val client = ctx.getBean(WordCountClient::class.java)
            client.publishSentence("test to test for words")

            val listener = ctx.getBean(WordCountListener::class.java)

            Awaitility.await().atMost(10, TimeUnit.SECONDS).until {
                listener.getWordCounts().size == 4 &&
                        listener.getCount("test") == 2L &&
                        listener.getCount("to") == 1L &&
                        listener.getCount("for") == 1L &&
                        listener.getCount("words") == 1L
            }
        }
    }
}
