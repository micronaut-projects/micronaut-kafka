package io.micronaut.kafka.docs.consumer.config

import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import io.micronaut.kafka.docs.Product
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.Map

class ConfigProductListenerTest {

    @Test
    fun testSendProduct() {
        ApplicationContext.run(
            mapOf("kafka.enabled" to StringUtils.TRUE, "spec.name" to "ConfigProductListenerTest")
        ).use { ctx ->
            Assertions.assertDoesNotThrow {
                val product = Product("Blue Trainers", 5)
                val client = ctx.getBean(ProductClient::class.java)
                client.send("Nike", product)
            }
        }
    }
}
