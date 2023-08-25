package io.micronaut.kafka.docs.consumer.config;

import io.micronaut.context.ApplicationContext;
import io.micronaut.kafka.docs.Product;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ConfigProductListenerTest {

    @Test
    void testSendProduct() {
        try (ApplicationContext ctx = ApplicationContext.run(
            Map.of("kafka.enabled", "true", "spec.name", "ConfigProductListenerTest")
        )) {
            Product product = new Product("Blue Trainers", 5);
            ProductClient client = ctx.getBean(ProductClient.class);
            client.receive("Nike", product);
        }
    }
}
