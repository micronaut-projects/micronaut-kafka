package io.micronaut.kafka.docs.admin;

import io.micronaut.configuration.kafka.admin.KafkaNewTopics;
import io.micronaut.context.ApplicationContext;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MyTopicFactoryTest {

    @Test
    void testNewTopics() throws ExecutionException, InterruptedException {
        try (ApplicationContext ctx = ApplicationContext.run(Map.of(
            "kafka.enabled", "true",
            "spec.name", "MyTopicFactoryTest"
        ))) {
            final KafkaNewTopics newTopics = ctx.getBean(KafkaNewTopics.class);
            await().atMost(5, SECONDS).until(() -> areNewTopicsDone(newTopics));
            final CreateTopicsResult result = newTopics.getResult().orElseThrow();
            assertEquals(1, result.numPartitions("my-new-topic-1").get());
            assertEquals(2, result.numPartitions("my-new-topic-2").get());
        }
    }

    // tag::result[]
    boolean areNewTopicsDone(KafkaNewTopics newTopics) {
        return newTopics.getResult().map(result -> result.all().isDone()).orElse(false);
    }
    // end::result[]
}
