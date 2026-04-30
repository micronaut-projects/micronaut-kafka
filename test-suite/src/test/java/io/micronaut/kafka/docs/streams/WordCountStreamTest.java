package io.micronaut.kafka.docs.streams;

import io.micronaut.context.ApplicationContext;
import io.micronaut.core.util.StringUtils;
import io.micronaut.testcontainers.kafka.Kafka;
import org.junit.jupiter.api.Test;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

class WordCountStreamTest {

    @Test
    void testWordCounter() throws IOException {
        Map<String, String> kafkaProps = Kafka.getProperties();
        Map<String, Object> config = new HashMap<>(kafkaProps);
        String uniqueSuffix = UUID.randomUUID().toString();
        Path stateDir = Files.createTempDirectory("test-suite-java-word-count-stream-");
        stateDir.toFile().deleteOnExit();
        config.put("kafka.enabled", StringUtils.TRUE);
        config.put("micronaut.application.name", "test-suite-java-word-count-stream");
        config.put("kafka.streams.default.application.id", "test-suite-java-word-count-stream-" + uniqueSuffix);
        config.put("kafka.streams.default.state.dir", stateDir.toString());
        config.put("spec.name", "WordCountStreamTest");
        config.put("kafka.streams.my-stream.application.id", "test-suite-java-my-stream-" + uniqueSuffix);
        config.put("kafka.streams.my-stream.start-kafka-streams", StringUtils.FALSE);
        config.put("kafka.streams.my-other-stream.application.id", "test-suite-java-my-other-stream-" + uniqueSuffix);
        config.put("kafka.streams.my-other-stream.start-kafka-streams", StringUtils.FALSE);

        try (ApplicationContext ctx = ApplicationContext.run(config)) {
            await().atMost(30, SECONDS).until(() -> {
                var states = ctx.getBeansOfType(KafkaStreams.class).stream().map(KafkaStreams::state).toList();
                return states.size() == 3
                    && states.stream().filter(State::isRunningOrRebalancing).count() == 1
                    && states.stream().filter(state -> state == State.CREATED).count() == 2;
            });

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
