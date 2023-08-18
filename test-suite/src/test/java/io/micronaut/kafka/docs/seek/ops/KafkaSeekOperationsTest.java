package io.micronaut.kafka.docs.seek.ops;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.kafka.docs.Product;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@MicronautTest
@Property(name = "spec.name", value = "KafkaSeekOperationsTest")
class KafkaSeekOperationsTest {
    @Test
    void testProductListener(ProductClient producer, ProductListener consumer) {
        Product product0 = new Product("Apple", 10);
        Product product1 = new Product("Banana", 20);

        producer.produce(product0);
        producer.produce(product1);

        await().atMost(5, SECONDS).until(() ->
            consumer.processed.contains(product0) &&
            !consumer.processed.contains(product1)
        );
    }

    @Requires(property = "spec.name", value = "KafkaSeekOperationsTest")
    @KafkaClient
    interface ProductClient {
        @Topic("awesome-products")
        void produce(Product product);
    }
}
