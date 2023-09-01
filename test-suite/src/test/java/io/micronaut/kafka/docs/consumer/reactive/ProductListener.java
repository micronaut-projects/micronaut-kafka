package io.micronaut.kafka.docs.consumer.reactive;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Blocking;
import io.micronaut.kafka.docs.Product;
import org.slf4j.Logger;
import reactor.core.publisher.Mono;

import static org.slf4j.LoggerFactory.getLogger;

@Requires(property = "spec.name", value = "ReactiveProductListenerTest")
@KafkaListener
public class ProductListener {
    private static final Logger LOG = getLogger(ProductListener.class);

    // tag::method[]
    @Topic("reactive-products")
    public Mono<Product> receive(@KafkaKey String brand,  // <1>
                                 Mono<Product> productPublisher) { // <2>
        return productPublisher.doOnSuccess((product) ->
            LOG.info("Got Product - {} by {}", product.name(), brand) // <3>
        );
    }
    // end::method[]

    // tag::blocking[]
    @Blocking
    @Topic("reactive-products")
    public Mono<Product> receiveBlocking(@KafkaKey String brand, Mono<Product> productPublisher) {
        return productPublisher.doOnSuccess((product) ->
            LOG.info("Got Product - {} by {}", product.name(), brand)
        );
    }
    // end::blocking[]
}
