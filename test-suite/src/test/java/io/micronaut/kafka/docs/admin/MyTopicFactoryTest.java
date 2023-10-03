package io.micronaut.kafka.docs.admin;

import io.micronaut.configuration.kafka.admin.KafkaNewTopics;
import io.micronaut.context.ApplicationContext;
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
            assertEquals(1, newTopics.getResult().numPartitions("my-new-topic-1").get());
            assertEquals(2, newTopics.getResult().numPartitions("my-new-topic-2").get());
        }
    }

    // tag::result[]
    boolean areNewTopicsDone(KafkaNewTopics newTopics) {
        return newTopics.getResult().all().isDone();
    }
    // end::result[]
}
