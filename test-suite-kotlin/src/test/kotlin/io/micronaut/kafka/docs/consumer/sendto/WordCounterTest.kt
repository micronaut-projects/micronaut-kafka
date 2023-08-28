package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.context.ApplicationContext
import org.awaitility.Awaitility
import org.junit.jupiter.api.Test
import java.util.Map
import java.util.concurrent.TimeUnit

class WordCounterTest {

    @Test
    fun testWordCounter() {
        ApplicationContext.run(
            Map.of<String, Any>("kafka.enabled", "true", "spec.name", "WordCounterTest")
        ).use { ctx ->
            val client = ctx.getBean(WordCounterClient::class.java)
            client.send("test to test for words")
            val listener: WordCountListener = ctx.getBean(WordCountListener::class.java)
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until {
                listener.wordCount.size         == 4 &&
                listener.wordCount.get("test")  == 2 &&
                listener.wordCount.get("to")    == 1 &&
                listener.wordCount.get("for")   == 1 &&
                listener.wordCount.get("words") == 1
            }
        }
    }
}
