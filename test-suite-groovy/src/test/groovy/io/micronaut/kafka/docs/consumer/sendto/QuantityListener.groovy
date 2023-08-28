package io.micronaut.kafka.docs.consumer.sendto

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires

@Slf4j
@Requires(property = 'spec.name', value = 'SendToProductListenerTest')
@KafkaListener(offsetReset = OffsetReset.EARLIEST)
class QuantityListener {

    Integer quantity

    @Topic("product-quantities")
    int receive(@KafkaKey String brand, Integer quantity) {
        log.info("Got Quantity - {} by {}", quantity, brand)
        this.quantity = quantity
    }
}
