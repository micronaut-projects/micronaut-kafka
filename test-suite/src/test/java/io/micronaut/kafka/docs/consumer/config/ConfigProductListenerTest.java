package io.micronaut.kafka.docs.consumer.config;

import io.micronaut.context.ApplicationContext;
import io.micronaut.kafka.docs.Product;
import io.micronaut.testcontainers.kafka.Kafka;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class ConfigProductListenerTest {

    @Test
    void testSendProduct() {
        Map<String, String> kafkaProps = Kafka.getProperties();

        Map<String, Object> config = new HashMap<>(kafkaProps);
        config.put("kafka.enabled", "true");
        config.put("spec.name", "ConfigProductListenerTest");

        try (ApplicationContext ctx = ApplicationContext.run(config)) {
            assertDoesNotThrow(() -> {
                Product product = new Product("Blue Trainers", 5);
                ProductClient client = ctx.getBean(ProductClient.class);
                client.send("Nike", product);
            });
        }
    }
}
