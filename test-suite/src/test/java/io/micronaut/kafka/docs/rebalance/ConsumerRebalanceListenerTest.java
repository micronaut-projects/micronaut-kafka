package io.micronaut.kafka.docs.rebalance;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.kafka.docs.AbstractKafkaTest;
import io.micronaut.kafka.docs.Product;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@MicronautTest
@Property(name = "spec.name", value = "ConsumerRebalanceListenerTest")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsumerRebalanceListenerTest extends AbstractKafkaTest {
    @Test
    void testProductListener(ProductClient producer, ProductListener consumer) {
        Product product0 = new Product("Apple", 10);
        Product product1 = new Product("Banana", 20);

        producer.produce(product0);
        producer.produce(product1);

        await().atMost(5, SECONDS).until(() ->
            !consumer.processed.contains(product0) &&
            consumer.processed.contains(product1)
        );

        MY_KAFKA.stop();
    }

    @Requires(property = "spec.name", value = "ConsumerRebalanceListenerTest")
    @KafkaClient
    interface ProductClient {
        @Topic("awesome-products")
        void produce(Product product);
    }
}
