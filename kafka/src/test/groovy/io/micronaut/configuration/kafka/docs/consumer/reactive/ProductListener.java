package io.micronaut.configuration.kafka.docs.consumer.reactive;

import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.configuration.kafka.docs.consumer.config.Product;
import io.reactivex.Single;

@KafkaListener
public class ProductListener {

    // tag::method[]
    @Topic("reactive-products")
    public Single<Product> receive(@KafkaKey String brand,  // <1>
                                   Single<Product> productFlowable) { // <2>
        return productFlowable.doOnSuccess((product) ->
                System.out.println("Got Product - " + product.getName() + " by " + brand) // <3>
        );
    }
    // end::method[]
}
