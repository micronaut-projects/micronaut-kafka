package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product
import io.micronaut.kafka.docs.consumer.batch.BookListener
import io.micronaut.messaging.annotation.SendTo
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.function.Function

@Requires(property = "spec.name", value = "SendToProductListenerTest")
@KafkaListener(offsetReset = OffsetReset.EARLIEST)
class QuantityListener {

    companion object {
        private val LOG = LoggerFactory.getLogger(BookListener::class.java)
    }

    var quantity : Integer? = null

    @Topic("product-quantities")
    fun receive(@KafkaKey brand: String?, quantity: Integer) {
        LOG.info("Got Quantity - {} by {}", quantity, brand)
        this.quantity = quantity
    }
}
