package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.context.ApplicationContext
import io.micronaut.kafka.docs.Product
import org.awaitility.Awaitility
import org.junit.jupiter.api.Test
import java.util.Map
import java.util.concurrent.TimeUnit

class SendToProductListenerTest {

    @Test
    fun testSendProduct() {
        ApplicationContext.run(
            Map.of<String, Any>("kafka.enabled", "true", "spec.name", "SendToProductListenerTest")
        ).use { ctx ->
            val product = Product("Blue Trainers", 5)
            val client = ctx.getBean(ProductClient::class.java)
            client.send("Nike", product)
            val listener = ctx.getBean(QuantityListener::class.java)
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until {
                listener.quantity != null &&
                listener.quantity!!.toInt() == 5
            }
        }
    }
}
