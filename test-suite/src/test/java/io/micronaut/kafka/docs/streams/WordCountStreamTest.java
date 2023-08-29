package io.micronaut.kafka.docs.streams;

import io.micronaut.context.ApplicationContext;
import io.micronaut.core.util.StringUtils;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

class WordCountStreamTest {

    @Test
    void testWordCounter() {
        try (ApplicationContext ctx = ApplicationContext.run(
            Map.of("kafka.enabled", StringUtils.TRUE, "spec.name", "WordCountStreamTest")
        )) {
            WordCountClient client = ctx.getBean(WordCountClient.class);
            client.publishSentence("test to test for words");
            WordCountListener listener = ctx.getBean(WordCountListener.class);
            await().atMost(10, SECONDS).until(() ->
                listener.getWordCounts().size()       == 4 &&
                    listener.getCount("test")  == 2 &&
                    listener.getCount("to")    == 1 &&
                    listener.getCount("for")   == 1 &&
                    listener.getCount("words") == 1
            );
        }
    }
}
