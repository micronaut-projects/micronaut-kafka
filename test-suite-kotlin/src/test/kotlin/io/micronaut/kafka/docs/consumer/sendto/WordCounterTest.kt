package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.context.ApplicationContext
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.Map

class WordCounterTest {

    @Test
    fun testWordCounter() {
        ApplicationContext.run(
            Map.of<String, Any>("kafka.enabled", "true", "spec.name", "WordCounterTest")
        ).use { ctx ->
            Assertions.assertDoesNotThrow {
                val client = ctx.getBean(WordCounterClient::class.java)
                client.send("Test to test for words")
            }
        }
    }
}
