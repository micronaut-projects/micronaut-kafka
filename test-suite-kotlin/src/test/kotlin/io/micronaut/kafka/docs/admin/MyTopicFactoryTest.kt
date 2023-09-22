package io.micronaut.kafka.docs.admin

import io.micronaut.configuration.kafka.admin.KafkaNewTopics
import io.micronaut.context.ApplicationContext
import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

internal class MyTopicFactoryTest {

    @Test
    @Throws(ExecutionException::class, InterruptedException::class)
    fun testNewTopics() {
        ApplicationContext.run(
            mapOf(
                "kafka.enabled" to "true",
                "spec.name" to "MyTopicFactoryTest"
            )
        ).use { ctx ->
            val newTopics = ctx.getBean(KafkaNewTopics::class.java)
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until { areNewTopicsDone(newTopics) }
            val result = newTopics.result.orElseThrow()
            assertEquals(1, result.numPartitions("my-new-topic-1").get())
            assertEquals(2, result.numPartitions("my-new-topic-2").get())
        }
    }

    // tag::result[]
    fun areNewTopicsDone(newTopics: KafkaNewTopics): Boolean {
        return newTopics.result.map { it.all().isDone }.orElse(false)
    }
    // end::result[]
}
