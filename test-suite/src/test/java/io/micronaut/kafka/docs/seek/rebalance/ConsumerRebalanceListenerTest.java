package io.micronaut.kafka.docs.seek.rebalance;

import io.micronaut.context.annotation.Property;
import io.micronaut.kafka.docs.Products;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@MicronautTest
@Property(name = "spec.name", value = "ConsumerRebalanceListenerTest")
class ConsumerRebalanceListenerTest {
    @Test
    void testProductListener(ProductListener consumer) {
        await().atMost(10, SECONDS).until(() ->
            !consumer.processed.contains(Products.PRODUCT_0) &&
            consumer.processed.contains(Products.PRODUCT_1)
        );
    }
}
