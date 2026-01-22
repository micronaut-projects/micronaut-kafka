package io.micronaut.kafka.docs.seek.rebalance;

import io.micronaut.context.annotation.Property;
import io.micronaut.kafka.docs.Products;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import io.micronaut.testcontainers.kafka.Kafka;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@MicronautTest
@Property(name = "spec.name", value = "ConsumerRebalanceListenerTest")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsumerRebalanceListenerTest implements TestPropertyProvider {

    @Override
    public Map<String, String> getProperties() {
        return Kafka.getProperties();
    }

    @Test
    void testProductListener(ProductListener consumer) {
        await().atMost(10, SECONDS).until(() ->
            !consumer.processed.contains(Products.PRODUCT_0) &&
            consumer.processed.contains(Products.PRODUCT_1)
        );
    }
}
