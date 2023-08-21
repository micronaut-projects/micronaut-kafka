package io.micronaut.kafka.docs.consumer.sendto

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.consumer.batch.BookListener
import io.micronaut.messaging.annotation.SendTo
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.util.function.Function
// end::imports[]

@Requires(property = "spec.name", value = "ProductListenerTest")
@KafkaListener
class ProductListener {

    companion object {
        private val LOG = LoggerFactory.getLogger(BookListener::class.java)
    }

    // tag::method[]
    @Topic("awesome-products") // <1>
    @SendTo("product-quantities") // <2>
    fun receive(@KafkaKey brand: String?, product: Product): Int {
        LOG.info("Got Product - {} by {}", product.name, brand)
        return product.quantity // <3>
    }
    // end::method[]

    // tag::reactive[]
    @Topic("awesome-products") // <1>
    @SendTo("product-quantities") // <2>
    fun receiveProduct(@KafkaKey brand: String?, productSingle: Mono<Product>): Mono<Int> {
        return productSingle.map(Function<Product, Int> { product: Product ->
            LOG.info("Got Product - {} by {}", product.name, brand)
            product.quantity // <3>
        })
    }
// end::reactive[]
}
