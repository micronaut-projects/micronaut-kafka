package io.micronaut.kafka.docs.consumer.config;

import io.micronaut.context.ApplicationContext;
import io.micronaut.kafka.docs.Product;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class ConfigProductListenerTest {

    @Test
    void testSendProduct() {
        try (ApplicationContext ctx = ApplicationContext.run(
            Map.of("kafka.enabled", "true", "spec.name", "ConfigProductListenerTest")
        )) {
            assertDoesNotThrow(() -> {
                Product product = new Product("Blue Trainers", 5);
                ProductClient client = ctx.getBean(ProductClient.class);
                client.send("Nike", product);
            });
        }
    }
}
