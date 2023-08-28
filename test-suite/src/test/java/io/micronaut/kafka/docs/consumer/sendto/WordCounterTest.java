package io.micronaut.kafka.docs.consumer.sendto;

import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class WordCounterTest {

    @Test
    void testWordCounter() {
        try (ApplicationContext ctx = ApplicationContext.run(
            Map.of("kafka.enabled", "true", "spec.name", "WordCounterTest")
        )) {
            WordCounterClient client = ctx.getBean(WordCounterClient.class);
            client.send("test to test for words");
            WordCountListener listener = ctx.getBean(WordCountListener.class);
            await().atMost(10, SECONDS).until(() ->
                listener.wordCount.size()       == 4 &&
                listener.wordCount.get("test")  == 2 &&
                listener.wordCount.get("to")    == 1 &&
                listener.wordCount.get("for")   == 1 &&
                listener.wordCount.get("words") == 1
            );
        }
    }
}
