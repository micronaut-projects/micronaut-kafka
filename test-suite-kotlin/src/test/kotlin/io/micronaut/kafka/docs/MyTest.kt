package io.micronaut.kafka.docs

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.TimeUnit

@Property(name = "spec.name", value = "MyTest")
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class MyTest : AbstractKafkaTest() {

    @Test
    fun testKafkaRunning(producer: MyProducer, consumer: MyConsumer) {
        val message = "hello"
        producer.produce(message)
        await().atMost(5, TimeUnit.SECONDS).until { consumer.consumed == message }
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
        var consumed: String? = null

        @Topic("my-topic")
        fun consume(message: String) {
            consumed = message
        }
    }
}
