package io.micronaut.kafka.docs;

import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@MicronautTest
class MyTest extends AbstractKafkaTest {

    @Inject
    MyProducer producer;
    @Inject
    MyConsumer consumer;

    @Test
    void testKafkaRunning() {
        // Given
        final String message = "hello";
        // When
        producer.produce(message);
        // Then
        await().atMost(5, SECONDS).until(() -> message.equals(consumer.consumed));
        // Cleanup
        MY_KAFKA.stop();
    }

    @KafkaClient
    public interface MyProducer {
        @Topic("my-topic")
        void produce(String message);
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    public static class MyConsumer {
        String consumed;
        @Topic("my-topic")
        public void consume(String message) {
            consumed = message;
        }
    }
}
