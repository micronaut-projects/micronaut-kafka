package io.micronaut.kafka.docs.seek.ops

import io.micronaut.context.annotation.Property
import io.micronaut.kafka.docs.Products
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

@MicronautTest
@Property(name = "spec.name", value = "KafkaSeekOperationsTest")
internal class KafkaSeekOperationsTest {
    @Test
    fun testProductListener(consumer: ProductListener) {
        await().atMost(10, TimeUnit.SECONDS).until {
            consumer.processed.contains(Products.PRODUCT_0) &&
            !consumer.processed.contains(Products.PRODUCT_1)
        }
    }
}
