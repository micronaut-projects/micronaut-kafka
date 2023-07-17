package io.micronaut.kafka.docs

import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.util.concurrent.*

@MicronautTest
internal class MyTest : AbstractKafkaTest() {
    @Inject
    var producer: MyProducer? = null

    @Inject
    var consumer: MyConsumer? = null

    @Test
    fun testKafkaRunning() {
        // Given
        val message = "hello"
        // When
        producer!!.produce(message)
        // Then
        await().atMost(5, TimeUnit.SECONDS)
            .until(Callable<Boolean> { message == consumer!!.consumed })
        // Cleanup
        MY_KAFKA.stop()
    }

    @KafkaClient
    interface MyProducer {
        @Topic("my-topic")
        fun produce(message: String)
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    class MyConsumer {
        var consumed: String? = null

        @Topic("my-topic")
        fun consume(message: String) {
            consumed = message
        }
    }
}
