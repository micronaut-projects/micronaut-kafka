package io.micronaut.kafka.docs.seek.ops

import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.context.annotation.*
import io.micronaut.kafka.docs.AbstractKafkaTest
import io.micronaut.kafka.docs.Product
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.*
import java.util.concurrent.*

@MicronautTest
@Property(name = "spec.name", value = "KafkaSeekOperationsTest")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class KafkaSeekOperationsTest : AbstractKafkaTest() {
    @Test
    fun testProductListener(producer: ProductClient, consumer: ProductListener) {
        val product0 = Product("Apple", 10)
        val product1 = Product("Banana", 20)

        producer.produce(product0)
        producer.produce(product1)

        await().atMost(5, TimeUnit.SECONDS).until {
            consumer.processed.contains(product0) &&
            !consumer.processed.contains(product1)
        }

        MY_KAFKA.stop()
    }

    @Requires(property = "spec.name", value = "KafkaSeekOperationsTest")
    @KafkaClient
    interface ProductClient {
        @Topic("awesome-products")
        fun produce(product: Product)
    }
}
