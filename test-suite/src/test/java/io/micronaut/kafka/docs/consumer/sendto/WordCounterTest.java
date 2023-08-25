package io.micronaut.kafka.docs.consumer.sendto;

import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class WordCounterTest {

    @Test
    void testWordCounter() {
        try (ApplicationContext ctx = ApplicationContext.run(
            Map.of("kafka.enabled", "true", "spec.name", "WordCounterTest")
        )) {
            assertDoesNotThrow(() -> {
                WordCounterClient client = ctx.getBean(WordCounterClient.class);
                client.send("Test to test for words");
            });
        }
    }
}
