package io.micronaut.kafka.docs.consumer.reactive

import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Blocking
import reactor.core.publisher.Mono

@Requires(property = "spec.name", value = "ReactiveProductListenerTest")
@KafkaListener
class ProductListener {

    // tag::method[]
    @Topic("reactive-products")
    fun receive(@KafkaKey brand: String?,  // <1>
        productFlowable: Mono<Product>): Mono<Product> { // <2>
        return productFlowable.doOnSuccess { (name): Product ->
            println("Got Product - $name by $brand") // <3>
        }
    }
    // end::method[]

    // tag::blocking[]
    @Blocking
    @Topic("reactive-products")
    fun receiveBlocking(@KafkaKey brand: String?, productFlowable: Mono<Product>): Mono<Product> {
        return productFlowable.doOnSuccess { (name): Product ->
            println("Got Product - $name by $brand")
        }
    }
    // end::blocking[]
}
