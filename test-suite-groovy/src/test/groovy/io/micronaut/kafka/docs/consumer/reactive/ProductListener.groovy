package io.micronaut.kafka.docs.consumer.reactive

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Blocking
import io.micronaut.kafka.docs.Product
import reactor.core.publisher.Mono

@Requires(property = 'spec.name', value = 'ReactiveProductListenerTest')
@Slf4j
@KafkaListener
class ProductListener {

    // tag::method[]
    @Topic('reactive-products')
    Mono<Product> receive(@KafkaKey String brand,  // <1>
                          Mono<Product> productFlowable) { // <2>
        return productFlowable.doOnSuccess((product) ->
                log.info("Got Product - {} by {}", product.name(), brand) // <3>
        )
    }
    // end::method[]

    // tag::blocking[]
    @Blocking
    @Topic('reactive-products')
    Mono<Product> receiveBlocking(@KafkaKey String brand, Mono<Product> productFlowable) {
        return productFlowable.doOnSuccess((product) ->
                log.info("Got Product - {} by {}", product.name(), brand)
        )
    }
    // end::blocking[]
}
