package io.micronaut.kafka.docs.consumer.sendto;

import io.micronaut.context.ApplicationContext;
import io.micronaut.kafka.docs.Product;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

class SendToProductListenerTest {

    @Test
    void testSendProduct() {
        try (ApplicationContext ctx = ApplicationContext.run(
            Map.of("kafka.enabled", "true", "spec.name", "SendToProductListenerTest")
        )) {
            Product product = new Product("Blue Trainers", 5);
            ProductClient client = ctx.getBean(ProductClient.class);
            client.send("Nike", product);
            QuantityListener listener = ctx.getBean(QuantityListener.class);
            await().atMost(10, SECONDS).until(() ->
                listener.quantity != null &&
                listener.quantity == 5
            );
        }
    }
}
