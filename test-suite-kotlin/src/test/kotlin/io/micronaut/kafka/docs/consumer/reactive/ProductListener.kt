package io.micronaut.kafka.docs.consumer.reactive

import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Blocking
import io.micronaut.kafka.docs.Product
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

@Requires(property = "spec.name", value = "ReactiveProductListenerTest")
@KafkaListener
class ProductListener {

    companion object {
        private val LOG = LoggerFactory.getLogger(ProductListener::class.java)
    }

    // tag::method[]
    @Topic("reactive-products")
    fun receive(@KafkaKey brand: String,  // <1>
        product: Mono<Product>): Mono<Product> { // <2>
        return product.doOnSuccess { (name): Product ->
            LOG.info("Got Product - {} by {}", name, brand) // <3>
        }
    }
    // end::method[]

    // tag::blocking[]
    @Blocking
    @Topic("reactive-products")
    fun receiveBlocking(@KafkaKey brand: String, product: Mono<Product>): Mono<Product> {
        return product.doOnSuccess { (name): Product ->
            LOG.info("Got Product - {} by {}", name, brand)
        }
    }
    // end::blocking[]
}
