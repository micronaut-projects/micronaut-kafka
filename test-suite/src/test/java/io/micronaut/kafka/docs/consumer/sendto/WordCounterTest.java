package io.micronaut.kafka.docs.consumer.sendto;

import io.micronaut.context.ApplicationContext;
import io.micronaut.testcontainers.kafka.Kafka;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

class WordCounterTest {

    @Test
    void testWordCounter() {
        Map<String, String> kafkaProps = Kafka.getProperties();

        Map<String, Object> config = new HashMap<>(kafkaProps);
        config.put("kafka.enabled", "true");
        config.put("spec.name", "WordCounterTest");

        try (ApplicationContext ctx = ApplicationContext.run(config)) {
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
