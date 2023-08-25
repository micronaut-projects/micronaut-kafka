package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.context.ApplicationContext
import io.micronaut.kafka.docs.Product
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.Map

class SendToProductListenerTest {

    @Test
    fun testSendProduct() {
        ApplicationContext.run(
            Map.of<String, Any>("kafka.enabled", "true", "spec.name", "SendToProductListenerTest")
        ).use { ctx ->
            Assertions.assertDoesNotThrow {
                val product = Product("Blue Trainers", 5)
                val client = ctx.getBean(ProductClient::class.java)
                client.send("Nike", product)
            }
        }
    }
}
