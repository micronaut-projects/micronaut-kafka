package io.micronaut.kafka.docs.streams;

import io.micronaut.context.ApplicationContext;
import io.micronaut.core.util.StringUtils;
import io.micronaut.testcontainers.kafka.Kafka;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

class WordCountStreamTest {

    @Test
    void testWordCounter() {
        Map<String, String> kafkaProps = Kafka.getProperties();
        Map<String, Object> config = new HashMap<>(kafkaProps);
        config.put("kafka.enabled", StringUtils.TRUE);
        config.put("spec.name", "WordCountStreamTest");
        config.put("kafka.streams.my-stream.application.id", "test-suite-java-my-stream");
        config.put("kafka.streams.my-stream.start-kafka-streams", StringUtils.FALSE);
        config.put("kafka.streams.my-other-stream.application.id", "test-suite-java-my-other-stream");
        config.put("kafka.streams.my-other-stream.start-kafka-streams", StringUtils.FALSE);

        try (ApplicationContext ctx = ApplicationContext.run(config)) {
            WordCountClient client = ctx.getBean(WordCountClient.class);
            client.publishSentence("test to test for words");

            WordCountListener listener = ctx.getBean(WordCountListener.class);

            await().atMost(30, SECONDS).until(() ->
                listener.getWordCounts().size() == 4 &&
                    listener.getCount("test")  == 2 &&
                    listener.getCount("to")    == 1 &&
                    listener.getCount("for")   == 1 &&
                    listener.getCount("words") == 1
            );
        }
    }
}
