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
import kotlinx.coroutines.delay

@Property(name = "spec.name", value = "KotlinCoroutinesTest")
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class KotlinCoroutinesTest : AbstractKafkaTest() {

    @Test
    fun testSuspendConsumer(producer: MyProducer, suspendConsumer: SuspendConsumer) {
        val message = "hello"
        producer.produce(message)
        await().atMost(5, TimeUnit.SECONDS).until { suspendConsumer.consumed == message }
    }

    @Requires(property = "spec.name", value = "KotlinCoroutinesTest")
    @KafkaClient
    interface MyProducer {
        @Topic("my-topic")
        fun produce(message: String)
    }

    @Requires(property = "spec.name", value = "KotlinCoroutinesTest")
    @KafkaListener(groupId = "suspend-group",offsetReset = OffsetReset.EARLIEST)
    class SuspendConsumer {
        var consumed: String? = null

        @Topic("my-topic")
        suspend fun consume(message: String) {
            consumed = message
            delay(10)
        }
    }
}
