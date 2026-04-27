package io.micronaut.kafka.docs

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

@Property(name = "spec.name", value = "MyTest")
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class MyTest : AbstractKafkaTest() {

    @Test
    fun testKafkaRunning(producer: MyProducer, consumer: MyConsumer) {
        val message = "hello"
        producer.produce(message)
        assertEquals(message, consumer.awaitMessage(15, TimeUnit.SECONDS))
    }

    @Requires(property = "spec.name", value = "MyTest")
    @KafkaClient
    interface MyProducer {
        @Topic("my-topic")
        fun produce(message: String)
    }

    @Requires(property = "spec.name", value = "MyTest")
    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    class MyConsumer {
        private val consumedMessages = LinkedBlockingQueue<String>()

        @Topic("my-topic")
        fun consume(message: String) {
            consumedMessages.offer(message)
        }

        fun awaitMessage(timeout: Long, timeUnit: TimeUnit): String? = consumedMessages.poll(timeout, timeUnit)
    }
}
