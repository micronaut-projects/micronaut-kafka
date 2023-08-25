package io.micronaut.kafka.docs.streams;

import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class WordCountStreamTest {

    @Test
    void testWordCounter() {
        try (ApplicationContext ctx = ApplicationContext.run(
            Map.of("kafka.enabled", "true", "spec.name", "WordCountStreamTest")
        )) {
            assertDoesNotThrow(() -> {
                WordCountClient client = ctx.getBean(WordCountClient.class);
                client.publishSentence("Test to test for words");
            });
        }
    }
}
