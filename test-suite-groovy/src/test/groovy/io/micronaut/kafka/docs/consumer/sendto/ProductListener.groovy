package io.micronaut.kafka.docs.consumer.sendto

import groovy.util.logging.Slf4j

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product
import io.micronaut.messaging.annotation.SendTo
import reactor.core.publisher.Mono
// end::imports[]

@Slf4j
@Requires(property = 'spec.name', value = 'ProductListenerTest')
@KafkaListener
class ProductListener {

    // tag::method[]
    @Topic("awesome-products") // <1>
    @SendTo("product-quantities") // <2>
    int receive(@KafkaKey String brand, Product product) {
        log.info("Got Product - {} by {}", product.name, brand)
        product.quantity // <3>
    }
    // end::method[]

    // tag::reactive[]
    @Topic("awesome-products") // <1>
    @SendTo("product-quantities") // <2>
    Mono<Integer> receiveProduct(@KafkaKey String brand, Mono<Product> productSingle) {
        productSingle.map(product -> {
            log.info("Got Product - {} by {}", product.name, brand)
            return product.quantity // <3>
        })
    }
    // end::reactive[]
}
